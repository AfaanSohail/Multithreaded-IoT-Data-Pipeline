#include <iostream>
#include <string>
#include <queue>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>

using namespace std;

struct Data {
    int id;
    int bytes;
    int csvNo;
};

struct Chunk {
    int id;          
    string payload;  
};

struct SensorStats {
    float totalSum = 0;
    int count = 0;
    int anomalies = 0;
};

struct FinalRecord {
    char key[32];
    float average;
    int anomalies; 
};

// Global Variables 
queue<Chunk> taskQueue;
unordered_map<string, SensorStats> aggTable;

int numThreads = 4;
int queueSize = 100;
int fifoFd = -1;
string shmName = "";

pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapMutex = PTHREAD_MUTEX_INITIALIZER;
sem_t sem_empty;
sem_t sem_full;

// Reader Thread (reads data until the queue is full)
void* readerFunction(void*) {
    while (true) {
        Data header;
        // Read the data from the pipe
        if (read(fifoFd, &header, sizeof(Data)) <= 0){
            break;
        } 
        // Check for the EOF 
        if (header.id == -1) {
            // Poison Pill Implementation 
            for (int i = 0; i < numThreads; i++) {
                sem_wait(&sem_empty);
                pthread_mutex_lock(&queueMutex);
                // Push -1 into the queue to signal the rest of the threads
                taskQueue.push({-1, ""});
                pthread_mutex_unlock(&queueMutex);
                sem_post(&sem_full);
            }
            break; 
        }
        // Move the sensor values into a buffer
        char* buffer = new char[header.bytes + 1];
        read(fifoFd, buffer, header.bytes);
        buffer[header.bytes] = '\0'; 
        // Push the data read from the pipe into the queue
        sem_wait(&sem_empty);               
        pthread_mutex_lock(&queueMutex);    
        taskQueue.push({header.id, string(buffer)});           
        pthread_mutex_unlock(&queueMutex);  
        sem_post(&sem_full);                

        delete[] buffer;
    }
    return NULL;
}

// Worker threads (Consume the data from the queue if its not empty)
void* workerFunction(void* arg) {
    // Take out the chunk of data from the queue
    while (true) {
        sem_wait(&sem_full);                
        pthread_mutex_lock(&queueMutex);    
        Chunk myTask = taskQueue.front();
        taskQueue.pop();                    
        pthread_mutex_unlock(&queueMutex);  
        sem_post(&sem_empty);     
        // Check for the Poison Pill from the reader
        if (myTask.id == -1){
            break;
        } 
        // Used for parsing the string effectively
        stringstream ss(myTask.payload);
        string line;
        // Use a localMap for each thread to avoid locking and unlocking overhead for the global map
        unordered_map<string, SensorStats> localMap; 
        // Parse the values from the sensor
        while (getline(ss, line)) {
            if (line.empty()){
                continue;
            }
            stringstream lineStream(line);
            string key, valueStr;
            // First column is the key of the sensor
            if (getline(lineStream, key, ',')) {
                // Get the values of the sensor
                while (getline(lineStream, valueStr, ',')) {

                    try {
                        float val = stof(valueStr);
                        // If we are inserting the data of a specific sensor for the first time then we dont have to check for anomalies
                        if (localMap[key].count > 0) {
                            float currentAverage = localMap[key].totalSum / localMap[key].count;
                            if (val > currentAverage * 1.5 || val < currentAverage * 0.5) {
                                localMap[key].anomalies++;
                            }
                        }
                        localMap[key].totalSum += val;
                        localMap[key].count++;
                    } 
                    // If there is bad sensor data (that may cause the stof to crash) then catch the exception gracefully
                    catch (...) { }
                }
            }
        }
        // Lock the global map once per thread
        pthread_mutex_lock(&mapMutex);
        // Move the data from the localmap to the global map
        for (auto const& pair : localMap) {
            aggTable[pair.first].totalSum += pair.second.totalSum;
            aggTable[pair.first].count += pair.second.count;
            aggTable[pair.first].anomalies += pair.second.anomalies;
        }
        // Unlock the global map
        pthread_mutex_unlock(&mapMutex);
    }
    delete (int*)arg; 
    return NULL;
}

int main(int argc, char *argv[]) {
    string fifoPath;
    int opt;
    // Parse the arguments 
    while ((opt = getopt(argc, argv, "n:q:f:s:")) != -1) {
        switch (opt) {
            case 'n': 
                numThreads = stoi(optarg); 
                break;
            case 'q': 
                queueSize = stoi(optarg); 
                break;
            case 'f': 
                fifoPath = optarg; 
                break;
            case 's': 
                shmName = optarg; 
                break;
            case '?': // Unknown arguments
            default:  
                exit(10); // Bad arguments
        }
    }
    
    // Check if required arguments are missing
    if (fifoPath.empty() || shmName.empty()){
        exit(10);
    } 

    // Initialize the first semaphore with queuesize
    sem_init(&sem_empty, 0, queueSize);
    // Initialize the second semaphore with 0
    sem_init(&sem_full, 0, 0);

    // Open the pipe for reading
    fifoFd = open(fifoPath.c_str(), O_RDONLY);
    if (fifoFd < 0){
        exit(40);
    }

    // Make the reader thread using thread attributes
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    // Make the reader thread joinable
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    pthread_t readerThread;
    pthread_create(&readerThread, &attr, readerFunction, NULL);

    // Make the worker threads using thread attributes
    vector<pthread_t> workers(numThreads);
    for (int i = 0; i < numThreads; i++) {
        int* id = new int(i + 1); 
        pthread_create(&workers[i], &attr, workerFunction, (void*)id);
    }
    pthread_attr_destroy(&attr); 
    // Call join for the worker and reader threads
    pthread_join(readerThread, NULL);
    for (int i = 0; i < numThreads; i++){
        pthread_join(workers[i], NULL);
    }
    // Opening the shared memory that the dispatcher created
    int shmFd = shm_open(shmName.c_str(), O_RDWR, 0666);
    if (shmFd < 0){
        exit(20); // Failed to open shared memory
    } 
    if (shmFd >= 0) {
        // Map the shared memory into the RAM
        void* ptr = mmap(0, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, 0);
        if (ptr == MAP_FAILED){
            exit(20); // Memory mapping failure
        } 
        FinalRecord* shmRecords = (FinalRecord*)ptr;
        
        int i = 0;
        // Copy the results of the aggregation to the shared memory
        for (auto const& pair : aggTable) {
            strncpy(shmRecords[i].key, pair.first.c_str(), 31);
            shmRecords[i].key[31] = '\0';
            shmRecords[i].average = pair.second.totalSum / pair.second.count;
            shmRecords[i].anomalies = pair.second.anomalies;
            i++;
        }
        // Store a null key at the end as a signal for the reporter
        shmRecords[i].key[0] = '\0'; 
        // Closing and unmapping
        munmap(ptr, 4096);
        close(shmFd);
    }
    // Opening the semaphore created in dispatcher
    sem_t* reporterSem = sem_open("/semo", 0);
    if (reporterSem == SEM_FAILED){
        exit(20); // Failed to open semaphore
    } 
    else {
        // Signal the reporter to start
        sem_post(reporterSem); 
        sem_close(reporterSem);
    }

    // Cleanup
    close(fifoFd);
    sem_destroy(&sem_empty);
    sem_destroy(&sem_full);
    return 0; 
}