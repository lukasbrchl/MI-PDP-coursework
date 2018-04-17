#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <deque>
#include <sstream>
#include <limits>
#include <algorithm>
#include <stack>
#include <dirent.h>
#include <chrono>
#include <stdexcept>
#include "mpi.h"
#include <unistd.h>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>


#define MSG_LENGTH 300
#define MAX_QUEUE_SIZE 30

#define MASTERS_RANK 0
#define STATE_TAG 1
#define READY_TAG 2
#define TERMINATION_TAG  3
#define UPPER_BOUND_TAG  4

using namespace std;

static const short KNIGHT_OFFSETS[8][2] = {{1,  -2},
                                           {2,  -1},
                                           {2,  1},
                                           {1,  2},
                                           {-1, 2},
                                           {-2, 1},
                                           {-2, -1},
                                           {-1, -2}};

class State {
public:

    State() {}

    State(const pair<int, int> &startingPos, const vector<pair<int, int>> &remainingFigs, const int steps) : remainingFigs(remainingFigs), steps(steps) {
        moves.push_back(startingPos);
    }

    State(const State *state) {
        moves = state->moves;
        remainingFigs = state->remainingFigs;
        steps = state->steps;
    }

    const vector<pair<int, int>> &getRemainingFigs() const { return remainingFigs; }

    const vector<pair<int, int>> &getMoves() const { return moves; }

    int getSteps() const { return steps; }

    void move(pair<int, int> &coords) {
        auto pos = lower_bound(remainingFigs.begin(), remainingFigs.end(), coords);
        if (pos != remainingFigs.end() && *pos == coords)
            remainingFigs.erase(pos);

        moves.push_back(coords);
        ++steps;
    }

    vector<pair<int, int>> getAvailableMoves(const int boardSize) const {
        vector<pair<int, int>> coords;
        coords.reserve(8);
        int knightX = moves.back().first, knightY = moves.back().second;

        for (int i = 0; i < 8; i++) {
            int x = knightX + KNIGHT_OFFSETS[i][0];
            int y = knightY + KNIGHT_OFFSETS[i][1];
            if (x < 0 || x >= boardSize || y < 0 || y >= boardSize) continue;

            coords.push_back(make_pair(x, y));
        }

        sort(begin(coords), end(coords), [this](const pair<int, int> &a, const pair<int, int> &b) { return getMovementPrice(a) < getMovementPrice(b); });
        return coords;
    }

    friend ostream &operator<<(ostream &os, const State &state) {
        os << "moves: ";
        for (auto it = state.moves.begin(); it != state.moves.end(); it++) {
            os << "{" << (*it).first << "," << (*it).second << "}, ";
        }
        os << "remainingFigs: ";
        for (auto it = state.remainingFigs.begin(); it != state.remainingFigs.end(); it++) {
            os << "{" << (*it).first << "," << (*it).second << "}, ";
        }
        os << "steps: " << state.steps << "\n";
        return os;
    }

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & moves;
        ar & remainingFigs;
        ar & steps;
    }

private:

    int getMovementPrice(const pair<int, int> &coords) const {
        return binary_search(remainingFigs.begin(), remainingFigs.end(), coords) ? 8 : 0;
    }

    int getClosestFigureDist(const pair<int, int> &coords) const {
        int bestDist = numeric_limits<int>::max();

        for (auto it = remainingFigs.begin(); it != remainingFigs.end(); it++) {
            int dist = abs(coords.first - (*it).first) + abs(coords.second - (*it).second);
            if (bestDist > dist) bestDist = dist;
        }

        return bestDist;
    }

    vector<pair<int, int>> moves;
    vector<pair<int, int>> remainingFigs;
    int steps;
};

class KnightProblem {
public:
    KnightProblem(const string fileName) : fileName(fileName) {
        MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
        MPI_Comm_size(MPI_COMM_WORLD, &processesCnt);
        ifstream file(fileName);

        if (!file.is_open()) {
            cout << "Unable to open file.\n";
            throw runtime_error("Unable to open file");
        }

        parseInputFile(file);
//        cout << "File " << fileName << " successfully parsed.\n";
        file.close();
    }

    ~KnightProblem() {
        if (solution != nullptr) delete solution;
    }

    void solve() {
        char message[MSG_LENGTH];
        MPI_Status status;

        deque<State *> deque;
        State *startState = new State(startingKnight, startingFigs, 0);
        solution = new State(startingKnight, startingFigs, upperBound);
        deque.push_back(startState);

        double startTime = MPI_Wtime();

        if (myRank == 0) {
            bool workersState[processesCnt]; // true means that he is ready for work
            for (int i = 0; i < processesCnt; i++) workersState[i] = true;

            while (deque.size() < MAX_QUEUE_SIZE) {
                State *state = deque.front();
                deque.pop_front();
                vector<pair<int, int>> moves = state->getAvailableMoves(boardSize);

                #pragma omp parallel for default ( shared )
                for (int i = 0; i < moves.size(); i++) {
                    State *newState = new State(state);
                    newState->move(moves[i]);
                    #pragma omp critical
                    deque.push_back(newState);
                }
            }

            while (!deque.empty()) {
                for (int workerId = 1; workerId < processesCnt; workerId++) {
                    if (deque.empty()) break;
                    if (!workersState[workerId]) continue;

                    State *state = deque.front();
                    deque.pop_front();

                    std::stringstream ss;
                    boost::archive::text_oarchive oa(ss);
                    oa << *state;
                    cout << "Sending work to slave " << workerId << endl;
                    MPI_Send(ss.str().c_str(), ss.str().size(), MPI_CHAR, workerId, STATE_TAG, MPI_COMM_WORLD);
                    workersState[workerId] = false;
                }

                int nonWorkingCnt = 0;
                for (int workerId = 1; workerId < processesCnt; workerId++) {
                    if (workersState[workerId])
                        nonWorkingCnt++;
                }


                for (int i = 0; i < processesCnt - 1 - nonWorkingCnt; i++) {
                    int length;
                    cout << "Master is waiting for slave's work to be done" << endl;
                    MPI_Recv(message, MSG_LENGTH, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    MPI_Get_count(&status, MPI_CHAR, &length);
                    cout << "Master recieved from worker " << status.MPI_SOURCE << " " << message << endl;

                    if (status.MPI_TAG == READY_TAG)
                        workersState[status.MPI_SOURCE] = true;
                }
            }

            bool next = false;
            while (!next) {
                int cnt = 1;
                for (int i = 1; i < processesCnt; i++) {
                    if (workersState[i] == true) cnt++;
                }
                if (cnt == processesCnt) break;
            }

            cout << "############# WORK IS FINISHED, SOLUTIONS CALLBACK ######################" << endl;

            for (int workerId = 1; workerId < processesCnt; workerId++) {
                strncpy(message, "terminate", sizeof(message));
                MPI_Send(message, strlen(message) + 1, MPI_CHAR, workerId, TERMINATION_TAG, MPI_COMM_WORLD);
                cout << "Master sent termination command to worker " << workerId << " (" << message << ")" << endl;
            }
            int length;
            cout << "############# RECEIVING SOLUTIONS FROM SLAVES ######################" << endl;

            for (int workerId = 1; workerId < processesCnt; workerId++) {
                MPI_Recv(message, MSG_LENGTH, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                cout << "Master recieved from worker " << status.MPI_SOURCE << " " << message << endl;

                std::stringstream ss;
                ss.write(message, MSG_LENGTH);
                boost::archive::text_iarchive ia(ss);

                State *state = new State();
                ia >> *state;
                cout << "Slave " << status.MPI_SOURCE << " computed " << *state << endl;
                if (solution->getSteps() > state->getSteps())
                    solution = new State(state);
            }

            elapsedTime = MPI_Wtime() - startTime;
            printBestSolution();

        } else { // Slave process
            bool terminate = false;

            while (!terminate) {
                MPI_Recv(message, MSG_LENGTH, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

                if (status.MPI_TAG == TERMINATION_TAG) {
                    std::stringstream ss;
                    boost::archive::text_oarchive oa(ss);
                    oa << *solution;
                    MPI_Send(ss.str().c_str(), ss.str().size(), MPI_CHAR, MASTERS_RANK, STATE_TAG, MPI_COMM_WORLD);
                    cout << "Slave " << myRank << " sent solution and terminated" << endl;
                    terminate = true;
                } else if (status.MPI_TAG == UPPER_BOUND_TAG) {
                    std::stringstream ss;
                    ss.write(message, MSG_LENGTH);
                    boost::archive::text_iarchive ia(ss);

                    State *receivedUpperBound = new State();
                    ia >> *receivedUpperBound;

                    if (receivedUpperBound->getSteps() >= solution->getSteps()) continue;

                    solution = receivedUpperBound;
                    cout << "Slave " << myRank << " has recieved a new upper bound  from " << status.MPI_SOURCE << endl;
                } else {
                    std::stringstream ss;
                    ss.write(message, MSG_LENGTH);
                    boost::archive::text_iarchive ia(ss);

                    State *oldSolution = new State(solution);
                    State *state = new State();
                    ia >> *state;
                    cout << "Slave " << myRank << " recieved work" << endl;

                    solveRec(state);

                    if (solution->getSteps() < oldSolution->getSteps()) {
                        std::stringstream ss;
                        boost::archive::text_oarchive oa(ss);
                        oa << *solution;
                        for (int workerId = 1; workerId < processesCnt; workerId++) {
                            if (workerId == myRank) continue;
                            cout << "Slave " << myRank << " is sending to " << workerId << " a new upper bound with " << solution->getSteps() << " steps" << endl;
                            MPI_Send(ss.str().c_str(), ss.str().size(), MPI_CHAR, workerId, UPPER_BOUND_TAG, MPI_COMM_WORLD);
                        }
                    }

                    cout << "Slave " << myRank << " is sending ready, because he done computation, solution is - " << *solution;
                    strncpy(message, "ready", sizeof(message));
                    MPI_Send(message, strlen(message) + 1, MPI_CHAR, MASTERS_RANK, READY_TAG, MPI_COMM_WORLD);
                }
            }
        }
    }

    void solveRec(const State *state) {
        vector<pair<int, int>> moves = state->getAvailableMoves(boardSize);

        #pragma omp parallel for default ( shared )
        for (int i = 0; i < moves.size(); i++) {

            State *newState = new State(state);
            newState->move(moves[i]);

            if ((newState->getSteps() + newState->getRemainingFigs().size()) >= solution->getSteps()) {
                delete newState;
                continue;
            }

            if (newState->getRemainingFigs().empty()) {
                if (solution->getSteps() > newState->getSteps()) {
                    #pragma omp critical
                    {
                        if (solution->getSteps() > newState->getSteps())
                            solution = new State(newState);
                    }
                }
                delete newState;
                continue;
            }
            solveRec(newState);
        }
        delete state;
    }

    void printBestSolution() {
        cout << "File=" << fileName << ", steps=" << solution->getSteps() << ", elapsedTime=" << elapsedTime << ", moves=";

        for (auto it = solution->getMoves().begin(); it != solution->getMoves().end(); it++) {
            cout << "(" << (*it).first << "," << (*it).second << ")";
            if (isInStartingSetup(*it)) cout << "*";
        }
        cout << endl;

    }

private:
    bool parseInputFile(ifstream &file) {
        string line;
        getline(file, line);
        istringstream iss(line);
        iss >> boardSize >> upperBound;

        for (int x = 0; x < boardSize; x++) {
            getline(file, line);
            iss.str(line);
            for (int y = 0; y < boardSize; y++) {
                switch (iss.get()) {
                    case '1' :
                        startingFigs.push_back(make_pair(x, y));
                        break;
                    case '3' :
                        startingKnight = {x, y};
                        break;
                    default:
                        break;
                }
            }
        }

//        cout << "Board size: " << boardSize << "\nNumber of startingFigs: " << numOfFigs << "\nUpper bound: " << upperBound << endl;
        return true;
    }

    bool isInStartingSetup(const pair<int, int> &coords) const {
        return binary_search(startingFigs.begin(), startingFigs.end(), coords);
    }

    string fileName;
    int boardSize, upperBound, iterations = 0;
    double elapsedTime;
    vector<pair<int, int>> startingFigs;
    pair<int, int> startingKnight;
    State *solution = nullptr;
    int myRank, processesCnt; // MPI things
};

vector<string> getFilePaths(string folder) {
    vector<string> filePaths;

    if (auto dir = opendir(folder.c_str())) {
        while (auto f = readdir(dir)) {
            if (!f->d_name || f->d_name[0] == '.') continue; // Skip everything that starts with a dot

            filePaths.push_back(folder + f->d_name);
        }
        closedir(dir);
    }
    return filePaths;
}

int main(int argc, char **argv) {
    if (argc != 2) throw std::invalid_argument("...");

    MPI_Init(&argc, &argv);
    KnightProblem kp(argv[1]);
    kp.solve();
    MPI_Finalize();

    return 0;
}
