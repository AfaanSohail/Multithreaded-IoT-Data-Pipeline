//Ingester Process

#include <iostream>
#include <fstream>     
#include <string>
#include <unistd.h>     
#include <fcntl.h>      
#include <signal.h>   
#include <stdlib.h>     

using namespace std;


struct Data {
    int id;
    int bytes;
    int csvNo;
};

// Global variables
int fifoFd = -1;
int totalFilesProcessed = 0;
int dataSent = 0;
int totalBytesSent = 0;
int keepRunning = 1; 

void CsvHaveEnded() {
    if (fifoFd != -1) {
        Data kaamKhatam;
        kaamKhatam.id = -1; // -this tells the processor that work is done so stop
        kaamKhatam.bytes = 0;
        kaamKhatam.csvNo = -1;
        
        write(fifoFd, &kaamKhatam, sizeof(Data));
        cout << "[Ingester PID: " << getpid() << "] Sent -1 tothe to pipe." << endl;
    }
}

void handleSigterm(int sig) {
    cout << "\n[Ingester PID: " << getpid() << "] Received SIGTERM. Shutting down cleanly." << endl;
    keepRunning = 0; // Break the loops naturally to close the file descriptors
}

void handleSigusr1(int sig) {
    cerr << "\n--- [Ingester Live Stats] ---" << endl;
    cerr << "Files Processed: " << totalFilesProcessed << endl;
    cerr << "Chunks Sent: " << dataSent << endl;
    cerr << "Total Bytes Sent: " << totalBytesSent << endl;
    cerr << "-----------------------------\n" << endl;
}


int main(int argc, char *argv[]) {
    // Register standard termination signal
    signal(SIGTERM, handleSigterm);
    signal(SIGUSR1, handleSigusr1);

    string fifoPath = "";

    // Parse arguments (We only need the pipe path)
    int opt;
    while ((opt = getopt(argc, argv, "f:")) != -1) {
        switch (opt) {
            case 'f': fifoPath = optarg; break;
        }
    }

    if (fifoPath.empty()) {
        cerr << "[Ingester ERROR] Missing aargument." << endl;
        exit(10);
    }

    cout << "[Ingester PID: " << getpid() << "] Waiting for Processor to open pipe." << endl;

    // Open the FIFO for only writing
    fifoFd = open(fifoPath.c_str(), O_WRONLY);
    if (fifoFd < 0) {
        perror("[Ingester ERROR] Failed to open FIFO");
        exit(40); 
    }

    cout << "[Ingester PID: " << getpid() << "] Pipe connected! Reading input.txt for csv file names" << endl;

    // Opening input.txt which contains the list of our CSV files
    ifstream indexFile("input.txt");
    if (!indexFile.is_open()) {
        cerr << "[Ingester ERROR] Could not open input.txt." << endl;
        close(fifoFd);
        exit(40);
    }

    int csvNo = 1;
    const int maxData = 1000; 
    string csvFilePath;

    while (getline(indexFile, csvFilePath) && keepRunning == 1) {
        cout << "[Ingester] Processing file: " << csvFilePath << endl;

        // Opening the CSV file
        ifstream file(csvFilePath.c_str());
        if (!file.is_open()) {
            cerr << "[Ingester ERROR] Cannot read CSV: " << csvFilePath << endl;
            continue; 
        }

        string line;
        string temp = "";
        int count = 0; 

        // Read the CSV line by line
        while (getline(file, line) && keepRunning == 1) {
            temp += line + "\n";
            count++;

            // If we hit 1000 rows wesend it
            if (count >= maxData) {
                Data package;
                package.id = ++dataSent;
                package.bytes = temp.size(); 
                package.csvNo = csvNo;

                write(fifoFd, &package, sizeof(Data));
                write(fifoFd, temp.c_str(), temp.size());

                totalBytesSent += package.bytes;
                
                temp = ""; 
                count = 0;
            }
        }

        if (temp.length() > 0 && keepRunning == 1) {
            Data package;
            package.id = ++dataSent;
            package.bytes = temp.size();
            package.csvNo = csvNo;

            write(fifoFd, &package, sizeof(Data));
            write(fifoFd, temp.c_str(), temp.size());
            totalBytesSent += package.bytes;
        }

        file.close();
        totalFilesProcessed++;
        csvNo++;
    }
    
    // Cleaing up
    indexFile.close();
    CsvHaveEnded();
    close(fifoFd);
    
    cout << "[Ingester PID: " << getpid() << "] Finished successfully. Exiting." << endl;
    return 0; 
}