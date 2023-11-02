#include <omp.h>  
#include <stdio.h>
#include <stdlib.h> 
#include <chrono>
#include <iostream> 
#include <fstream>
#include <map>
#include <sstream>
#include <vector>

using namespace std;

fstream mainDataset;

vector<vector<string>> matrizDeDados;

int main(int argc, char* argv[])
{
    auto start = std::chrono::steady_clock::now();
    
    

    mainDataset.open("dataset_00_1000_sem_virg.csv", fstream::in);

    if (mainDataset.is_open() == false) {
        return -1;
    }
   

    vector<string> nomesArquivos = { "cdtup.csv", "berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv", 
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };

    

    #pragma omp parallel
    {
        #pragma omp parallel for
        
            for (int i = 0; i < nomesArquivos.size(); ++i) {
            
                std::string nomeArquivo = nomesArquivos[i];
                std::fstream arquivo;
                arquivo.open(nomeArquivo, fstream::in | fstream::out | fstream::app);
           
            
                if (arquivo.is_open()) {


                    
                    arquivo.close();
                }
                else {
                    std::cout << "Erro ao abrir o arquivo: " << nomeArquivo << std::endl;
                }
            }
    }



    mainDataset.close();
    
    
    auto end = chrono::steady_clock::now();
    std::cout << "Tempo       : " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "ms" << endl;    // Ending of parallel region 

}
//  ler o dataset csv e inserir em uma matriz, ele retorna o número de linhas que leu;

int atualizarDataSet() {
    vector<string> dadosLocais;
    int numLinhas;
    for (numLinhas = 0; numLinhas < 500; numLinhas++) {
        for (int i = 0; i < 25; i++) {
            string dado;
            if (getline(mainDataset, dado, ',')) {
                return numLinhas;
            };
            dadosLocais.push_back(dado);
        }
        matrizDeDados.push_back(dadosLocais);
        dadosLocais.clear();
    }
    return numLinhas;
}