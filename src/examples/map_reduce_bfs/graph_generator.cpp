#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <assert.h>
#include <random>
#include <functional>
#include <climits>
#include <algorithm>
#include <queue>

using namespace std;

int main(int /*argc */, char* argv[]) {
    int N = stoi(argv[1]);
    int E = stoi(argv[2]);
    assert(E >= N-1);

    mt19937 rng(random_device{}());
    uniform_int_distribution<int> dist(0, N-1);

    auto rn = bind(dist, rng);

    vector<vector<int>> graph(N);
    for(int i = 1; i < N; i++) {
        int v = rn()%i;
        graph[v].push_back(i);
        graph[i].push_back(v);
    }
    for(int i = N; i < E; i++) {
        int v = rn();
        int u = v;
        while(u == v || find(begin(graph[v]), end(graph[v]), u) != end(graph[v]))
            u = rn();
        graph[u].push_back(v);
        graph[v].push_back(u);
    }

    std::ofstream out(argv[3]);

    for(int i = 0; i < N; i++) {
        out << i << std::endl;
        out << graph[i].size() << " ";
        for(auto v: graph[i]) out << v << " ";
        out << endl;
        out << ((i) ? INT_MAX : 0) << endl;
        out << ((i) ? 0 : 1) << endl;
    }

    // bfs
    bool V[1000000];
    int D[1000000];
    for(auto& k: V) k = false;
    queue<int> que;
    que.push(0);
    V[0] = true;
    D[0] = 0;
    while(!que.empty()) {
        int v = que.front();
        que.pop();

        for(auto u: graph[v]) {
            if(!V[u]) {
                V[u] = true;
                que.push(u);
                D[u] = D[v]+1;
            }
        }
    }
    for(int i = 0; i < N; i++) {
        cout << "Node: " << i << " - " << D[i] << endl;
    }
}
