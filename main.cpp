#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <sstream>
#include <limits>
#include <algorithm>
#include <stack>
#include <dirent.h>


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

    State(const pair<int, int> &startingPos, const vector<pair<int, int> > &remainingFigs, const int steps) : remainingFigs(remainingFigs), steps(steps) {
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

    vector<pair<int, int>> getAvailableMoves(const int boardSize) {
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

private:

    int getMovementPrice(const pair<int, int> &coords) const {
        return binary_search(remainingFigs.begin(), remainingFigs.end(), coords) ? 1 : 0;
    }

    int getClosestFigureDist(const pair<int, int> &coords) const {
        int bestDist = numeric_limits<int>::max();

        for (auto it = remainingFigs.begin(); it != remainingFigs.end(); it++) {
            int dist = abs(coords.first - (*it).first) + abs(coords.second - (*it).second);
            if (bestDist > dist) bestDist = dist;
        }

        return bestDist;
    }

    vector<pair<int, int> > moves;
    vector<pair<int, int> > remainingFigs;
    int steps;
};

class KnightProblem {
public:
    KnightProblem(const string fileName) : fileName(fileName) {
        ifstream file(fileName);

        if (!file.is_open()) {
            cout << "Unable to open file.\n";
            throw runtime_error("Unable to open file");
        }

        parseInputFile(file);
//        cout << "File " << fileName << " successfully parsed.\n";
        file.close();
    }

    void solve() {
        clock_t begin = clock();

        stack<State *> stack;
        State *startState = new State(startingKnight, startingFigs, 0);
        solution = new State(startingKnight, startingFigs, upperBound);

        stack.push(startState);

        while (!stack.empty()) {
            ++iterations;
            State *state = stack.top();
            stack.pop();

//            if (state->getSteps() >= upperBound || state->getSteps() + state->getRemainingFigs().size() >= solution->getSteps()) {
//                delete state;
//                continue;
//            }

            vector<pair<int, int>> moves = state->getAvailableMoves(boardSize);

            for (auto it = moves.begin(); it != moves.end(); it++) {
                if (state->getSteps() < upperBound) {
                    State *newState = new State(state);

                    newState->move(*it);

                    if (newState->getRemainingFigs().empty()) {
                        if (solution->getSteps() > newState->getSteps())
                            solution = new State(newState);
                        else {
                            delete newState;
                            continue;
                        }
                    } else if ((newState->getSteps() + newState->getRemainingFigs().size()) >= solution->getSteps()) {
                        delete newState;
                        continue;
                    }

                    stack.push(newState);
                } else {
                    delete state;
                }
            }
        }
        elapsedTime = double(clock() - begin) / CLOCKS_PER_SEC;
        printBestSolution();
    }

    void printBestSolution() {
        cout << "File=" << fileName << ", steps=" << solution->getSteps() << ", iterations=" << iterations << ", elapsedTime=" << elapsedTime << ", moves=";
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
    State *solution;
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

int main() {

    string folder = "data/";
    vector<string> filePaths = getFilePaths(folder);

//    for (auto it = filePaths.begin(); it != filePaths.end(); it++) {
//        KnightProblem kp(*it);
//        kp.solve();
//    }

    KnightProblem kp1("data/kun01.txt");
//    KnightProblem kp2("data/kun02.txt");
//    KnightProblem kp3("data/kun03.txt");
//    KnightProblem kp4("data/kun04.txt");
//    KnightProblem kp5("data/kun05.txt");
//    KnightProblem kp6("data/kun06.txt");
//    KnightProblem kp7("data/kun07.txt");
//    KnightProblem kp8("data/kun08.txt");
//    KnightProblem kp9("data/kun09.txt");
//    KnightProblem kp10("data/kun10.txt");
//    KnightProblem kp11("data/kun11.txt");
//    KnightProblem kp12("data/kun12.txt");


    kp1.solve();
//    kp2.solve();
//    kp3.solve();
//    kp4.solve();
//    kp5.solve();
//    kp6.solve();
//    kp7.solve();
//    kp8.solve();
//    kp9.solve();
//    kp10.solve();
//    kp11.solve();
//    kp12.solve();

    return 0;
}