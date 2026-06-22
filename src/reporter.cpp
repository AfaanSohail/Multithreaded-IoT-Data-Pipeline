// Reporter Process

#include <iostream>
#include <fstream>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <signal.h>
#include <cstdlib>
#include <cstring>
#include <cstdio> 

using namespace std;

// format of data that is being written into shm by processor
struct FinalRecord {
    char key[32];
    float average;
    int anomalies;
};

int main(int argc, char *argv[]) {
    string shmName  = "";
    string outputDir = "";

    // parsing arguments from cli -s shared memory name, -o output directory
    int opt;
    while ((opt = getopt(argc, argv, "s:o:")) != -1) {
        switch (opt) {
            case 's': shmName   = optarg; break;
            case 'o': outputDir = optarg; break;
            default:  exit(10);
        }
    }

    if (shmName.empty() || outputDir.empty()) {
        cerr << "Missing required arguments! ~ Reporter" << endl;
        exit(10);
    }

    cout << "[Reporter PID: " << getpid() << " | PPID: " << getppid() << "] Waiting on semaphore." << endl;

    sem_t* sem = sem_open("/semo", 0);
    if (sem == SEM_FAILED) {
        perror("sem_open failed! ~ Reporter");
        exit(20);
    }
    sem_wait(sem);          // pausing reporter until processor finishes writing to shm
    sem_close(sem);

    cout << "[Reporter PID: " << getpid() << "] Semaphore received! Reading shared memory ~ Reporter" << endl;

    // opening the shared memory segment created by processor
    int shmFd = shm_open(shmName.c_str(), O_RDONLY, 0666);
    if (shmFd < 0) {
        perror("[Reporter ERROR] shm_open failed");
        exit(20);
    }


    // mapping shm to our address space in read onlu mdoe
    void* ptr = mmap(0, 4096, PROT_READ, MAP_SHARED, shmFd, 0);
    if (ptr == MAP_FAILED) {
        perror("mmap failed! ~ Reporter");
        close(shmFd);
        exit(20);
    }
    close(shmFd);         // closing descriptor as it is no longer needed

    // typecasting from void* to our struct pointer
    FinalRecord* records = static_cast<FinalRecord*>(ptr);

    // couting total records entered by processor terminated by '\0' for loop later
    int count = 0;
    while (records[count].key[0] != '\0') {
        count++;
    }

    cout << "[Reporter PID: " << getpid() << "] Found " << count << " records in shared memory." << endl;

    // output file paths
    string reportTxtPath = outputDir + "/report.txt";
    string reportCsvPath = outputDir + "/report.csv";

    // -----------------------------------------------------------------------
    // dup() / dup2() demonstration for report.txt
    // We save stdout, redirect it to the file, write using printf, then restore.
    // -----------------------------------------------------------------------

    // creating a copy of the current stdout file descriptor
    int savedStdout = dup(STDOUT_FILENO);
    if (savedStdout < 0) {
        perror("dup failed! ~ Reporter");
        munmap(ptr, 4096);
        exit(40);
    }

    // opening the report.txt file in wrote mode
    int reportFd = open(reportTxtPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (reportFd < 0) {
        perror("Could not open report.txt! ~ Reporter");
        close(savedStdout);
        munmap(ptr, 4096);
        exit(40);
    }

    // redirecting stdout to report.txt to save output to file instead of terminal
    dup2(reportFd, STDOUT_FILENO);
    close(reportFd);

    // writing report summary...printf will write straight to our txt as we redirected fd
    printf("===== Sensor Aggregation Report =====\n");
    printf("%-30s %-12s %-10s\n", "Sensor ID", "Average", "Anomalies");
    printf("------------------------------------------------------\n");
    for (int i = 0; i < count; i++) {
        printf("%-30s %-12.2f %-10d\n", records[i].key, records[i].average, records[i].anomalies);
    }
    printf("------------------------------------------------------\n");
    printf("Total Records: %d\n", count);
    printf("=====================================\n");
    fflush(stdout); //fd restored back to pointing terminal

    dup2(savedStdout, STDOUT_FILENO);
    close(savedStdout); // no need for the copy now

    // writing report using fstream
    ofstream csvFile(reportCsvPath.c_str());
    if (!csvFile.is_open()) {
        cerr << "Could not open report.csv! ~ Reporter" << endl;
        munmap(ptr, 4096);
        exit(40);
    }

    csvFile << "sensor_id,average,anomalies\n";
    for (int i = 0; i < count; i++) {
        csvFile << records[i].key    << "," << records[i].average << "," << records[i].anomalies << "\n";
    }
    csvFile.close();

    munmap(ptr, 4096);   //cealning up mapped shm

    cout << "[Reporter PID: " << getpid() << "] report.txt and report.csv written to: " << outputDir << endl;

    // sending SIGUSR1 to the dispatcher to signal the pipeline is done
    kill(getppid(), SIGUSR1);

    cout << "[Reporter PID: " << getpid() << "] Sent SIGUSR1 to Dispatcher PID: " << getppid() << ". Exiting!" << endl;

    return 0;
}