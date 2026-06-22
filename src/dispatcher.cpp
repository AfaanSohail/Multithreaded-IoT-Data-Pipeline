// Dispatcher

#include <iostream>
#include <unistd.h>     
#include <sys/wait.h>   
#include <sys/stat.h>   
#include <fcntl.h>      
#include <sys/mman.h>   
#include <signal.h>
#include <string.h>     
#include <stdlib.h>     
#include <semaphore.h>  
#include <time.h>   

using namespace std;

string fifoPath = "";
string shmName = "";
string semName = "/semo"; 

// PID to keep track of our children
pid_t ingester = -1;
pid_t processor = -1;
pid_t reporter = -1;

// For Tracking 
int childrenActive = 0;
bool shutdownRequested = false;
time_t startTime;

void cleanPipes() {
    cout << "[Dispatcher PID: "<< getpid()<<"] Cleaning up IPC resources." << endl;
    if(!fifoPath.empty()) 
        unlink(fifoPath.c_str()); 
    if(!shmName.empty()) 
        shm_unlink(shmName.c_str());
    sem_unlink(semName.c_str());
}

void Terminate(int sig) {
    shutdownRequested = true;
    cout << endl<<"[Dispatcher PID: "<< getpid()<<"] Received shutdown signal (" << sig << ")." << endl;
    
    // Send terminate signal to children if they are still running
    if (ingester > 0) kill(ingester, SIGTERM);
    if (processor > 0) kill(processor, SIGTERM);
    if (reporter > 0) kill(reporter, SIGTERM);
    
    // Then clean up all the pipes we made
    cleanPipes();
    
    if (sig == SIGINT) exit(130);
    else exit(143);
}

void handleSigchld(int sig) {
    int status;
    pid_t pid;
    
    // WNOHANG so we don't block just check if the child exists
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        childrenActive--; // A child work is done
        
        time_t endTime = time(NULL);
        int runTime = endTime - startTime;

        // Terminal output on childs completion
        cout << "[Dispatcher] Child PID: " << pid << " | Exit Status: " << WEXITSTATUS(status) << " | Runtime: " << runTime << " seconds." << endl;

        // If a child died abnormally we shut everything down
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            cerr << "[Dispatcher ERROR] Child crashed. Initiating emergency shutdown." << endl;
            Terminate(SIGTERM);
        }
    }
}

void handleSigusr1(int sig) {
    cout << "[Dispatcher PID: " << getpid() << "] Received SIGUSR1 from Reporter. Pipeline complete." << endl;
}

pid_t spawnChild(const char* programName, const char* logFile, char* const programArgs[]) {
    pid_t pid = fork();
    
    if (pid < 0) {
        perror("[Dispatcher ERROR] fork failed");
        exit(30); // Exit Code 30: Process creation failure
    } 
    else if (pid == 0) {
        // --- WE ARE IN THE CHILD PROCESS ---
        
        // Open the log file for this specific process
        int logFd = open(logFile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (logFd < 0) exit(40); // I/O Error
        
        // Redirect stdout (1) and stderr (2) into the log file using dup2
        dup2(logFd, STDOUT_FILENO);
        dup2(logFd, STDERR_FILENO);
        close(logFd); // Close original descriptor, no longer needed

        // Replace the child process with the target program
        execvp(programName, programArgs);
        
        // If execvp returns, it failed to find/run the program!
        exit(30); 
    }
    
    // PARENT PROCESS 
    childrenActive++;
    return pid;
}

int main(int argc, char *argv[]) {
    // Seting Signal Handlers
    signal(SIGINT, Terminate);
    signal(SIGTERM, Terminate);
    signal(SIGCHLD, handleSigchld);
    signal(SIGUSR1, handleSigusr1);

    string inputDir, outputDir, numThreadsStr, queueSizeStr;
    
    // Parsing arguments using getopt
    int opt;
    while ((opt = getopt(argc, argv, "i:o:n:q:f:s:")) != -1) {
        switch (opt) {
            case 'i': inputDir = optarg; break;
            case 'o': outputDir = optarg; break;
            case 'n': numThreadsStr = optarg; break;
            case 'q': queueSizeStr = optarg; break;
            case 'f': fifoPath = optarg; break;
            case 's': shmName = optarg; break;
            default: exit(10); // If wrong argument we exit using the standard code
        }
    }

    if (inputDir.empty() || outputDir.empty() || numThreadsStr.empty() || queueSizeStr.empty() || fifoPath.empty() || shmName.empty()) {
        cerr << "[Dispatcher ERROR] Missing required arguments." << endl;
        exit(10);
    }

    // Creating FIFO 0666 provides read&write permissions
    if (mkfifo(fifoPath.c_str(), 0666) == -1) {
        perror("Failed to create FIFO");
        exit(20);
    }

    // Creating Shared Memory
    int shmFd = shm_open(shmName.c_str(), O_CREAT | O_RDWR, 0666);
    if (shmFd == -1) { 
        cleanPipes();
        exit(20); 
    }
    
    if (ftruncate(shmFd, 4096) == -1) {
         cleanPipes();
          exit(20); 
    }
    
   // Creating Shared Memory
    shmFd = shm_open(shmName.c_str(), O_CREAT | O_RDWR, 0666);
    if (shmFd == -1) { 
        cleanPipes();
        exit(20); 
    }
    
    // Seting the size of the shared memory to 4096 bytes
    if (ftruncate(shmFd, 4096) == -1) {
         cleanPipes();
         exit(20); 
    }
    
    close(shmFd);

    sem_t* semPtr = sem_open(semName.c_str(), O_CREAT, 0666, 0);
    if (semPtr == SEM_FAILED) {
        perror("Failed to create Semaphore");
        cleanPipes();
        exit(20);
    }
    sem_close(semPtr); 

    cout << "[Dispatcher PID: " << getpid() << "] IPC done now forking" << endl;

    startTime = time(NULL);

    // create the 3 Children for the 3 next processes and thena lso send arguments
    char* ingesterArgs[] = { (char*)"./src/ingester", (char*)"-i", (char*)inputDir.c_str(), (char*)"-f", (char*)fifoPath.c_str(), NULL };
    char* processorArgs[] = { (char*)"./src/processor", (char*)"-n", (char*)numThreadsStr.c_str(), (char*)"-q", (char*)queueSizeStr.c_str(), (char*)"-f", (char*)fifoPath.c_str(), (char*)"-s", (char*)shmName.c_str(), NULL };
    char* reporterArgs[] = { (char*)"./src/reporter", (char*)"-o", (char*)outputDir.c_str(), (char*)"-s", (char*)shmName.c_str(), NULL };

    ingester = spawnChild("./src/ingester", "logs/ingester.log", ingesterArgs);
    processor = spawnChild("./src/processor", "logs/processor.log", processorArgs);
    reporter = spawnChild("./src/reporter", "logs/reporter.log", reporterArgs);

    while (childrenActive > 0 && !shutdownRequested) {
        pause(); //we just wait till any thing new occurs
    }

    // Pipeline finished
    cleanPipes();
    return 0;
}