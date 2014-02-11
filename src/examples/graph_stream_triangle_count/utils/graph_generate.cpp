#include <random>
#include <unordered_map>
#include <utility>
#include <vector>
#include <iostream>
#include <algorithm>
#include <fstream>
#include <memory>

using namespace std;

vector<vector<int>> graph;

inline void add_edge(int u, int v) {
    graph[u][v] = 1;
    graph[v][u] = 1;
}

inline bool is_edge(int u, int v) {
    return graph[u][v];
}

int main(int argc, char* argv[]) {
    int vc = stoi(argv[1]);

    graph.resize(vc);
    for(auto& g: graph) {
        g.resize(vc);
        fill(begin(g), end(g), 0);
    }

    int fn = stoi(argv[2]);
    string bfname = "edges_";
    unique_ptr<ofstream> *files = new unique_ptr<ofstream>[fn];

    for(int i = 0; i < fn; i++) files[i].reset(new ofstream(bfname + to_string(i+1)));

    random_device rd;
    mt19937 rng(rd());
    uniform_int_distribution<int> dist(0, vc-1);
    for(int i = 0; i < vc*vc; i++) {
        if(dist(rng)%2) {
            int u = dist(rng);
            int v = u;
            while(v == u) v = dist(rng);
            if(!is_edge(u,v)) {
                add_edge(u,v);
                for(int k = 0; k < fn; k++) *files[k] << u << " " << v << endl;
            }
        } 
    }

    int counter = 0;
    for(int i = 0; i < vc; i++) {
        for(int j = 0; j < i; j++) {
            for(int k = 0; k < j; k++) {
                if(is_edge(i, j) && is_edge(j, k) && is_edge(i,k)) counter++;
            }
        }
    }
    std::cout << "Triangles: " << counter << std::endl;

    delete [] files;

    return 0;
}
