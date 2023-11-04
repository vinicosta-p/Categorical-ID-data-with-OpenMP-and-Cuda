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
fstream finalDataset;

vector<vector<string>> matrizDeDados;

int atualizarDataSet();

void escrita_do_dataset(vector<vector<string>> escritaMatriz);

void pairCodigoDescricao(string nomeArquivo);

int main(int argc, char* argv[])
{
    auto start = std::chrono::steady_clock::now();
    
    mainDataset.open("dataset_00_1000_sem_virg.csv", fstream::in);

    if (mainDataset.is_open() == false) {
        return -1;
    }
   

    vector<string> nomesArquivos = { "cdtup.csv", "berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv", 
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };

    
    int NUM_LINHAS_LIDAS = 0;
   
    #pragma omp parallel
    {   
       
        #pragma omp single
        {   
            if (NUM_LINHAS_LIDAS == 0)
            { 
                escrita_do_dataset(matrizDeDados);
            }
            NUM_LINHAS_LIDAS = atualizarDataSet();
        }
        

        #pragma omp barrier 

        #pragma omp parallel for
        
            for (int i = 0; i < nomesArquivos.size(); ++i) {
                
                pairCodigoDescricao(nomesArquivos[i]);
          
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
            getline(mainDataset, dado, ',');
            /*
            if (getline(mainDataset, dado, ',') is ios::eofbit) {
                return numLinhas;
            };
            */
            dadosLocais.push_back(dado);
        }
       
        matrizDeDados.push_back(dadosLocais);
        dadosLocais.clear();
    }
    return numLinhas;
}

void escrita_do_dataset(vector<vector<string>> escritaMatriz) {
    
    int NUM_LINHAS = escritaMatriz.size();
    int NUM_COLUM = 25;
    if (finalDataset.is_open() == false) {
        finalDataset.open("dataset_00_1000_sem_virg_FINAL.csv", fstream::app);
    }
  
    for (int i = 0; i < NUM_LINHAS; i++) {
        for (int j = 0; j < NUM_COLUM; j++) {
            finalDataset << escritaMatriz[i][j].c_str() << ',';
        }
    }

    finalDataset.close();
}


void pairCodigoDescricao(string nomeArquivo) {

    std::fstream arquivo;
    arquivo.open(nomeArquivo, fstream::in | fstream::out | fstream::app);


    if (arquivo.is_open()) {
        
       
        arquivo.close();
    }
    else {
        std::cout << "Erro ao abrir o arquivo: " << nomeArquivo << std::endl;
    }
}