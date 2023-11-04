/*
* Esse c�digo trocar� os dados categ�ricos por dados id e criar� um dataset final com a troca
* Ele ir� criar arquivos com os dados categ�ricos e o c�digos deles, um para cada coluna;
* formato do dicion�rio: id,nome\n
* A forma como esse c�digo faz essa troca tem a seguinte estrutura:
*           
*           LOOP -> acaba quando n�o houver mais linhas para ler
*           {
*               1.(Thread �nica) l� o dataset e armazena em uma matriz com um tamanho x(a definir) de linhas e 26 colunas
*               2. distribui as threads para que elas 
*                   -> criem os arquivos coluna.csv
*                   -> leem a matriz, verificam se o valor lido est� no arquivo ** SE N�O ESTIVER insere o valor no arquivo ** e troca a string da matriz para o id
*                   -> esperam as outras threads
*               3.(Thread �nica) escreve o arquivo final
* 
*           Recursos criados:
*               var de nome das colunas 
*               dicion�rio com o valor das index da coluna dos dados categ�ricos
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
    Esse map de consulta para saber o nome da coluna e o index que est� associado.
    esse map � utilizado na fun��o pairCodigoDescricao()
*/
void criarMapComNomeDaColunaAndPosicao() {
    vector<int> numeroDaColuna = { 1, 2, 3, 5, 6, 7, 8, 17, 18, 20, 23 };
    int numColum = 0;
    for (int i = 0; i < nomesArquivos.size(); ++i) {
        idxColuna.insert(pair<string, int>(nomesArquivos[i], numeroDaColuna[numColum]));
        numColum++;
    }

}

//  ler o dataset csv e inserir em uma matriz, ele retorna o n�mero de linhas que leu;
int atualizarDataSet() {
    vector<string> dadosLocais;
    
    int numLinhas;
    // NUMERO DE LINHAS DA MATRIZ: escolhido de forma �rbitria podendo aumentar ou diminuir
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

// Escreve o datasetFinal toda vez que a matriz � atualizada
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