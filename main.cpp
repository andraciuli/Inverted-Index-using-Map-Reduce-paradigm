#include <iostream>
#include <fstream>
#include <pthread.h>
#include <queue>
#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>
#include <cstring>

using namespace std;

class FileQueue {
private:
    queue<string> files;
    pthread_mutex_t mtx;

public:
    FileQueue() {
        pthread_mutex_init(&mtx, nullptr);
    }

    ~FileQueue() {
        pthread_mutex_destroy(&mtx);
    }

    void push(const string &file) {
        pthread_mutex_lock(&mtx);
        files.push(file);
        pthread_mutex_unlock(&mtx);
    }

    bool pop(string &file) {
        pthread_mutex_lock(&mtx);
        if (files.empty()) {
            pthread_mutex_unlock(&mtx);
            return false;
        }
        file = files.front();
        files.pop();
        pthread_mutex_unlock(&mtx);
        return true;
    }
};

struct MapperHelper {
    vector<pair<string, int>> partialList;
    pthread_mutex_t m_mutex;

    MapperHelper() {
        pthread_mutex_init(&m_mutex, nullptr);
    }

    ~MapperHelper() {
        pthread_mutex_destroy(&m_mutex);
    }
};

struct ReducerHelper {
    map<char, map<string, vector<int>>> aggregatedList;
    pthread_mutex_t r_mutex;

    ReducerHelper() {
        pthread_mutex_init(&r_mutex, nullptr);
    }

    ~ReducerHelper() {
        pthread_mutex_destroy(&r_mutex);
    }
};

struct ThreadInfo {
    int numMappers;
    int numReducers;                                  
    int threadId;  
};

struct ThreadData {
    FileQueue *fileQueue;                           
    MapperHelper *mapperHelper;                      
    map<string, int> *fileIndices;                    
    pthread_barrier_t *mappers_barrier;
    ThreadInfo *threadInfo;                      
    ReducerHelper *reducerHelper;  
    pthread_barrier_t *reducers_barrier;                
};

string fixWord(const string &word) {
    string fixed;
    for (char c : word) {
        if (isalpha(c)) {
            fixed += tolower(c);
        }
    }
    return fixed;
}

void manageFileContent(ifstream &fileContent, int fileId, MapperHelper &mapperHelper) {
    // For every file we keep a map with the words and the file index
    map<string, int> fileResults;
        string word;
        while (fileContent >> word) {
            string fixedWord = fixWord(word);
            // If the word is not in the map, we add it
            if (fileResults.find(fixedWord) == fileResults.end()) {
                fileResults[fixedWord] = fileId;
            }
        }
        fileContent.close();

        // Then the words from the current file are added to the partial list
        // We need a mutex to avoid threads writing at the same time
        pthread_mutex_lock(&mapperHelper.m_mutex);
        for (const auto &[word, fileId] : fileResults) {
            mapperHelper.partialList.push_back({word, fileId});
        }
        pthread_mutex_unlock(&mapperHelper.m_mutex);
}

void *mapper(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    FileQueue &fileQueue = *(data->fileQueue);
    MapperHelper &mapperHelper = *(data->mapperHelper);
    map<string, int> &fileIndices = *(data->fileIndices);
    pthread_barrier_t &barrier = *(data->mappers_barrier);

    // Every thread processes a file from the queue implemented with mutex
    string file;
    while (fileQueue.pop(file)) {
        ifstream inFile(file);

        if (!inFile.is_open()) {
            cerr << "Could not open input file: " << file << endl;
            return nullptr;
        }
        // Get the file index
        int fileId = fileIndices[file];

        manageFileContent(inFile, fileId, mapperHelper);
    }

    // Wait until every mapper thread reached this point
    pthread_barrier_wait(&barrier);

    return nullptr;
}

vector<int> getLetterInterval(int numReducers, int threadId) {
    // Calculate the interval of letters that a reducer thread will process
    // Based on the number of reducers and the thread id
    int totalLetters = 26;
    int lettersPerThread = totalLetters / numReducers;
    int extraLetters = totalLetters % numReducers;

    int startLetterIdx, endLetterIdx;
    // If there are extra letters, the first extraLetters reducers will process lettersPerThread + 1 letters
    if (threadId < extraLetters) {
        startLetterIdx = threadId * (lettersPerThread + 1);
        endLetterIdx = startLetterIdx + lettersPerThread + 1;
    } else {
        startLetterIdx = threadId * lettersPerThread + extraLetters;
        endLetterIdx = startLetterIdx + lettersPerThread;
    }

    return {startLetterIdx, endLetterIdx};
}

void processLettersPerThread(int startLetterIdx, int endLetterIdx, map<char, map<string, vector<int>>> &aggregatedList) {
    for (int i = startLetterIdx; i < endLetterIdx; ++i) {
        char letter = 'a' + i;
        string fileName(1, letter);
        fileName += ".txt";
        ofstream outFile(fileName);

        // Check if there are words starting with the current letter
        auto it = aggregatedList.find(letter);
        if (it != aggregatedList.end()) {
            vector<pair<string, vector<int>>> sortedWords(it->second.begin(), it->second.end());

            // Sort the words for the current letter based on the number of files they appear in
            // If the number of files is the same, sort them alphabetically
            sort(sortedWords.begin(), sortedWords.end(), [](const auto &a, const auto &b) {
                if (a.second.size() != b.second.size()) {
                    return a.second.size() > b.second.size();
                }
                return a.first < b.first;
            });

            // Write the words and the files they appear in the output file
            for (const auto &[word, fileIds] : sortedWords) {
                outFile << word << ":[";
                for (size_t j = 0; j < fileIds.size(); ++j) {
                    outFile << fileIds[j];
                    if (j != fileIds.size() - 1) outFile << " ";
                }
                outFile << "]\n";
            }
        }
        outFile.close();
    }
}

void processPartialList(ThreadInfo threadInfo, ReducerHelper &reducerHelper, vector<pair<string, int>> allResults, pthread_barrier_t *reducers_barrier) {
    map<char, map<string, vector<int>>> &aggregatedList = reducerHelper.aggregatedList;
    
    int numReducers = threadInfo.numReducers;
    int numMappers = threadInfo.numMappers;
    int threadId = threadInfo.threadId - numMappers;

    // Every thread handles a part of the results from the mappers
    // StartIndex and EndIndex are calculate with the formula from the lab
    size_t totalSize = allResults.size();
    int startIdx = threadId * totalSize / numReducers;
    int endIdx = min((threadId + 1) * totalSize / numReducers, totalSize);

    // Process the partial list in the alocated interval
    map<char, map<string, vector<int>>> localResults;
    for (size_t i = startIdx; i < endIdx; ++i) {
        const auto &[word, fileId] = allResults[i];
        char firstLetter = tolower(word[0]);
        localResults[firstLetter][word].push_back(fileId);
    }

    // Every thread writes its results in the aggregated list
    // A mutex is needed to avoid thread writing at the same time
    pthread_mutex_lock(&reducerHelper.r_mutex);
    for (const auto &[letter, wordsMap] : localResults) {
        for (const auto &[word, fileIds] : wordsMap) {
            auto &wordIds = (aggregatedList)[letter][word];
            wordIds.insert(wordIds.end(), fileIds.begin(), fileIds.end());
            sort(wordIds.begin(), wordIds.end());
            wordIds.erase(unique(wordIds.begin(), wordIds.end()), wordIds.end());
        }
    }
    pthread_mutex_unlock(&reducerHelper.r_mutex);

    // Every reducer thread needs to finish processing the partial list before moving on
    pthread_barrier_wait(reducers_barrier);

    // Every reducer thread processes a part of the letters
    vector<int> letterInterval = getLetterInterval(numReducers, threadId);
    int startLetterIdx = letterInterval[0];
    int endLetterIdx = letterInterval[1];

    processLettersPerThread(startLetterIdx, endLetterIdx, aggregatedList);
}

void *reducer(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    MapperHelper &mapperHelper = *(data->mapperHelper);
    pthread_barrier_t *mappers_barrier = data->mappers_barrier;
    ReducerHelper &reducerHelper = *(data->reducerHelper);
    pthread_barrier_t *reducers_barrier = data->reducers_barrier;

    // Wait until every mapper thread reached this point
    pthread_barrier_wait(mappers_barrier);

    vector<pair<string, int>> allResults = mapperHelper.partialList;

    ThreadInfo &threadInfo = *(data->threadInfo);
    processPartialList(threadInfo, reducerHelper, allResults, reducers_barrier);

    return nullptr;
}


int main(int argc, char *argv[]) {
    int numMappers = stoi(argv[1]);
    int numReducers = stoi(argv[2]);
    string inputFile = argv[3];

    ifstream inFile(inputFile);
    if (!inFile.is_open()) {
        cerr << "Could not open input file: " << inputFile << endl;
        return 1;
    }

    int numFiles;
    inFile >> numFiles;
    vector<string> files(numFiles);
    map<string, int> fileIndices;
    for (int i = 0; i < numFiles; ++i) {
        inFile >> files[i];
        fileIndices[files[i]] = i + 1;
    }
    inFile.close();

    FileQueue fileQueue;
    MapperHelper mapperHelper = MapperHelper();
    ReducerHelper reducerHelper = ReducerHelper();

    // Initialize barriers for mappers and reducers
    pthread_barrier_t mappers_barrier;
    pthread_barrier_init(&mappers_barrier, nullptr, numMappers + numReducers);

    pthread_barrier_t reducers_barrier;
    pthread_barrier_init(&reducers_barrier, nullptr, numReducers);

    for (const auto &file : files) {
        fileQueue.push(file);
    }

    vector<pthread_t> threads(numMappers + numReducers);
    vector<ThreadData> threadData(numMappers + numReducers);
    vector<ThreadInfo> threadInfos(numMappers + numReducers);

    // Create threads for mappers and reducers
    for (int i = 0; i < numMappers + numReducers; ++i) {
        threadInfos[i] = {numMappers, numReducers, i};
        // For the first numMappers threads, we create a mapper thread and the rest are reducers
        if (i < numMappers) {
            threadData[i] = {&fileQueue, &mapperHelper, &fileIndices, &mappers_barrier, &threadInfos[i], &reducerHelper, &reducers_barrier};
            pthread_create(&threads[i], nullptr, mapper, &threadData[i]);
        } else {
            threadData[i] = {nullptr, &mapperHelper, nullptr, &mappers_barrier, &threadInfos[i], &reducerHelper, &reducers_barrier};
            pthread_create(&threads[i], nullptr, reducer, &threadData[i]);
        }
    }

    // Wait for all threads to finish
    for (int i = 0; i < numMappers + numReducers ; ++i) {
        pthread_join(threads[i], nullptr);
    }

    // Destroy barriers
    pthread_barrier_destroy(&mappers_barrier);
    pthread_barrier_destroy(&reducers_barrier);

    return 0;
}

