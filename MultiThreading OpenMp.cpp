/*
* Esse código trocará os dados categóricos por dados id e criará um dataset final com a troca
* Ele irá criar arquivos com os dados categóricos e o códigos deles, um para cada coluna;
* formato do dicionário: id,nome\n
* A forma como esse código faz essa troca tem a seguinte estrutura:
*           
*           LOOP -> acaba quando não houver mais linhas para ler
*           {
*               1.(Thread Única) lê o dataset e armazena em uma matriz com um tamanho x(a definir) de linhas e 26 colunas
*               2. distribui as threads para que elas 
*                   -> criem os arquivos coluna.csv
*                   -> leem a matriz, verificam se o valor lido está no arquivo ** SE NÃO ESTIVER insere o valor no arquivo ** e troca a string da matriz para o id
*                   -> esperam as outras threads
*               3.(Thread Única) escreve o arquivo final
* 
*           Recursos criados:
*               var de nome das colunas 
*               dicionário com o valor das index da coluna dos dados categóricos
* 
*           NOTAS:
*               Pensar nesse loop de forma individual(prioridade 1)
*               Pensar na busca de arquivo das threads(prioridade 2)
*/


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
map<string, int> idxColuna;

void criarMapComNomeDaColunaAndPosicao();

int atualizarDataSet();

void escrita_do_dataset(vector<vector<string>> escritaMatriz);

void pairCodigoDescricao(string nomeArquivo);

vector<string> nomesArquivos;

int main(int argc, char* argv[])
{
    auto start = std::chrono::steady_clock::now();

    nomesArquivos = { "cdtup.csv", "berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv",
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };
    

    mainDataset.open("dataset_00_1000_sem_virg.csv", fstream::in);

    if (mainDataset.is_open() == false) {
        return -1;
    }
   

    criarMapComNomeDaColunaAndPosicao();
    
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

        #pragma omp for nowait
            for (int i = 0; i < nomesArquivos.size(); ++i) {
                printf("%s\n", matrizDeDados[1][idxColuna[nomesArquivos[i]]].c_str());
                pairCodigoDescricao(nomesArquivos[i]);
          
            }
        
        #pragma omp barrier
    }



    mainDataset.close();
    
    
    auto end = chrono::steady_clock::now();
    std::cout << "Tempo       : " << chrono::duration_cast<chrono::microseconds>(end - start).count() << "ms" << endl;    // Ending of parallel region 

}
/*
    Esse map de consulta para saber o nome da coluna e o index que está associado.
    esse map é utilizado na função pairCodigoDescricao()
*/
void criarMapComNomeDaColunaAndPosicao() {
    vector<int> numeroDaColuna = { 1, 2, 3, 5, 6, 7, 8, 17, 18, 20, 23 };
    int numColum = 0;
    for (int i = 0; i < nomesArquivos.size(); ++i) {
        idxColuna.insert(pair<string, int>(nomesArquivos[i], numeroDaColuna[numColum]));
        numColum++;
    }

}

//  ler o dataset csv e inserir em uma matriz, ele retorna o número de linhas que leu;
int atualizarDataSet() {
    vector<string> dadosLocais;
    
    int numLinhas;
    // NUMERO DE LINHAS DA MATRIZ: escolhido de forma árbitria podendo aumentar ou diminuir
    const int NUM_DE_LINHAS_DA_MATRIZ = 500;

    for (numLinhas = 0; numLinhas < NUM_DE_LINHAS_DA_MATRIZ; numLinhas++) {
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

// Escreve o datasetFinal toda vez que a matriz é atualizada
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
    int NUM_COLUM = idxColuna[nomeArquivo];

    if (arquivo.is_open()) {
        
       
        arquivo.close();
    }
    else {
        std::cout << "Erro ao abrir o arquivo: " << nomeArquivo << std::endl;
    }
}