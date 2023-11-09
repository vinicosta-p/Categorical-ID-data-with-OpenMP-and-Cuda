/*
* Esse c�digo trocar� os dados categ�ricos por dados id e criar� um dataset final com a troca
* Ele ir� criar arquivos com os dados categ�ricos e o c�digos deles, um para cada coluna;
* formato do dicion�rio: id,nome\n
* A forma como esse c�digo faz essa troca tem a seguinte estrutura:
*           
*           LOOP -> acaba quando n�o houver mais linhas para ler
*           {
*               1.(Thread Principal) l� o dataset e armazena em uma matriz com um tamanho x(a definir) de linhas e 26 colunas
*               2.(MultiThread) distribui as threads para que elas 
*                   -> criem os arquivos coluna.csv
*                   -> leem a matriz, verificam se o valor lido est� no arquivo ** SE N�O ESTIVER insere o valor no arquivo ** e troca a string da matriz para o id
*                   -> esperam as outras threads
*               3.(Thread Principal) escreve o arquivo final
* 
*           Recursos criados:
*               var de nome das colunas 
*               dicion�rio com o valor das index da coluna dos dados categ�ricos
*/

/** Datased link: https://drive.google.com/file/d/1wfk_0QTIZA-uZktkOpwMmqFDVVIi5O-l/view?usp=sharing */

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

//Onde os dados s�o guardados
vector<vector<string>> matrizDeDados;
vector<string> nomesArquivos = {"berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv",
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };

//Dicion�rio que tem como chave o nome das colunas tratadas e o resultado
//� o index coluna utilizado na matriz de dados
map<string, int> idxColuna = {
    {"cdtup.csv", 1}, {"berco.csv", 2}, {"portoatracacao.csv", 3}, {"mes.csv", 5},
    {"tipooperacao.csv", 6}, {"tiponavegacaoatracacao.csv", 7}, {"terminal.csv", 8, },
    {"origem.csv", 17}, {"destino.csv", 18}, {"naturezacarga.csv", 20}, {"sentido.csv", 23}
};

const string DATASET_FILENAME = "dataset_00_1000_sem_virg.csv";
const string FINAL_DATASET_FILENAME = "final_dataset_00_1000_sem_virg.csv";
const int NUM_COLUNAS_CATEGORICAS = nomesArquivos.size();

//N�mero de linhas lidas dentro do while
int NUM_LINHAS_LIDAS = 0;

// NUMERO DE LINHAS DA MATRIZ: escolhido de forma �rbitria podendo aumentar ou diminuir
const int NUM_DE_LINHAS_DA_MATRIZ = 10000;

// vector com id e valor dos ultimos valores lidos 
// o primeiro indice � a coluna, o segundo � o vetor;
vector<map<string, string>> buscaRapidaDeDado;

bool fimDoArq = false;

int atualizarDataSet();

void escrita_do_dataset(vector<vector<string>> escritaMatriz);

void pairCodigoDescricao(string nomeArquivo, int indexDoArquivo);

signed char linhaInicial();

void limpaArquivo();

bool procuraNoCache(int indexDoArquivo, string dado, int linha, int coluna);

void inicializaMatriz_buscaRapidaDeDado();

int main(int argc, char* argv[]) {
    auto start = std::chrono::steady_clock::now();
    
    mainDataset.open(DATASET_FILENAME, fstream::in);

    if (mainDataset.is_open() == false) {
        return -1;
    }
    
    limpaArquivo();
    if (!linhaInicial()) {
        return -1;
    }

    inicializaMatriz_buscaRapidaDeDado();
    while (!fimDoArq) {
        NUM_LINHAS_LIDAS = atualizarDataSet();

        #pragma omp parallel
        {

            #pragma omp for nowait
            for (int i = 0; i < NUM_COLUNAS_CATEGORICAS; ++i) {
               pairCodigoDescricao(nomesArquivos[i], i);

            }
            #pragma omp barrier
        }

        escrita_do_dataset(matrizDeDados);
        matrizDeDados.clear();
    }

    mainDataset.close();
    auto end = chrono::steady_clock::now();
    printf("Tempo: %ldms\n", chrono::duration_cast<chrono::milliseconds>(end - start).count()); // Ending of parallel region 
}

void limpaArquivo() {
    int LINHA_ATUAL = 0;
    while (NUM_COLUNAS_CATEGORICAS < LINHA_ATUAL) {
        fstream arq;
        arq.open(nomesArquivos[LINHA_ATUAL], fstream::trunc);
        arq.close();
    }
};

/*
    Esse map de consulta para saber o nome da coluna e o index que est� associado.
    esse map � utilizado na fun��o pairCodigoDescricao()
*/
signed char linhaInicial() {
    finalDataset.open(FINAL_DATASET_FILENAME, fstream::app);
    if (!finalDataset.is_open()) {
        return 0;
    }

    string linha;
    getline(mainDataset, linha);

    finalDataset << linha;

    finalDataset.close();
    return 1;
}

void inicializaMatriz_buscaRapidaDeDado() {
    for (int i = 0; i < NUM_COLUNAS_CATEGORICAS; ++i) {
        map<string, string> aux; //= { nomesArquivos[i], "0" };
        buscaRapidaDeDado.push_back(aux);
    }
}

//  ler o dataset csv e inserir em uma matriz, ele retorna o n�mero de linhas que leu;
int atualizarDataSet() {
    vector<string> dadosLocais;
    int numLinhas;
    for (numLinhas = 0; numLinhas < NUM_DE_LINHAS_DA_MATRIZ; numLinhas++) {
        for (int i = 0; i < 25; i++) {
            string dado;
            //RESOLVER BUG DA �LTIMA LINHA NAO ADICIONAR '0\n'
            // solu��o temporia: adicionar = '0\n' na fun��o de escrita do dataset
            if (!getline(mainDataset, dado, ',')) {
                fimDoArq = true;
                printf("fim do arquivo %s\n", dado.c_str());
                return numLinhas;
            };
            dadosLocais.push_back(dado);
            dado.clear();
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
        finalDataset.open(FINAL_DATASET_FILENAME, fstream::app);
    }
  
    for (int i = 0; i < NUM_LINHAS; i++) {
        for (int j = 0; j < NUM_COLUM; j++) {
            finalDataset << escritaMatriz[i][j].c_str() << ',';
        }
    }

    //GAMBIARRA. ACHAR UMA SOLU��O MAIS LIMPA SEM QUEBRA DE FLUXO: O MOTIVO FOI A FUN��O DE GETLINE NAO ESTAVA PEGANDO O ULTIMO VALOR
    if (fimDoArq) {
        finalDataset << "0\n";
    }
    finalDataset.close();
}

bool procuraNoCache(int indexDoArquivo, string dado, int linha, int coluna) {
    bool naoVectorEstaVazio = !buscaRapidaDeDado[indexDoArquivo].empty();
    if (naoVectorEstaVazio) {
        if (buscaRapidaDeDado[indexDoArquivo].find(dado) != buscaRapidaDeDado[indexDoArquivo].end()) {
            matrizDeDados[linha][coluna] = buscaRapidaDeDado[indexDoArquivo][dado];
            return false;
        };
    }

    return true;
}

void pairCodigoDescricao(string nomeArquivo, int indexDoArquivo) {
    int NUM_COLUM = idxColuna[nomeArquivo];
    std::fstream arquivo;
    arquivo.open(nomeArquivo, fstream::app);
    for (int i = 0; i < NUM_LINHAS_LIDAS; i++) {
        string dado = matrizDeDados[i][NUM_COLUM];
        if (procuraNoCache(indexDoArquivo, dado, i, NUM_COLUM)) {
            int ultimoIndex = buscaRapidaDeDado[indexDoArquivo].size() + 1;
            buscaRapidaDeDado[indexDoArquivo][dado] = to_string(ultimoIndex);
            arquivo << to_string(ultimoIndex) << "," << dado << endl;
            matrizDeDados[i][NUM_COLUM] = to_string(ultimoIndex);
        }
    }
    arquivo.close();
}