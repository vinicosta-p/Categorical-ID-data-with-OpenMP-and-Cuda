/*
* Esse código trocará os dados categóricos por dados id e criará um dataset final com a troca
* Ele irá criar arquivos com os dados categóricos e o códigos deles, um para cada coluna;
* formato do dicionário: id,nome\n
* A forma como esse código faz essa troca tem a seguinte estrutura:
*           
*           LOOP -> acaba quando não houver mais linhas para ler
*           {
*               1.(Thread Principal) lê o dataset e armazena em uma matriz com um tamanho x(a definir) de linhas e 26 colunas
*               2.(MultiThread) distribui as threads para que elas 
*                   -> criem os arquivos coluna.csv
*                   -> leem a matriz, verificam se o valor lido está no arquivo ** SE NÃO ESTIVER insere o valor no arquivo ** e troca a string da matriz para o id
*                   -> esperam as outras threads
*               3.(Thread Principal) escreve o arquivo final
* 
*           Recursos criados:
*               var de nome das colunas 
*               dicionário com o valor das index da coluna dos dados categóricos
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
//Onde os dados são guardados
vector<vector<string>> matrizDeDados;

vector<string> nomesArquivos = { "cdtup.csv", "berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv",
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };
//Dicionário que tem como chave o nome das colunas tratadas e o resultado
//é o index coluna utilizado na matriz de dados
map<string, int> idxColuna;

//Número de linhas lidas dentro do while
int NUM_LINHAS_LIDAS = 0;


// vector com id e valor dos ultimos valores lidos 
// o primeiro indice é a coluna, o segundo é o vetor;
vector<map<string, string>> buscaRapidaDeDado;

bool fimDoArq = false;;

void criarMapComNomeDaColunaAndPosicao();

int atualizarDataSet();

void escrita_do_dataset(vector<vector<string>> escritaMatriz);

void pairCodigoDescricao(string nomeArquivo, int indexDoArquivo);

void linhaInicial();

void limpaArquivo() {
    for (int i = 0; i < nomesArquivos.size(); ++i) {
        fstream arq;
        arq.open(nomesArquivos[i], fstream::out);
       // arq.clear();
        arq.close();
    }
};


bool procuraNoCache(int indexDoArquivo, string dado, int linha, int coluna);

void inicializaMatriz_buscaRapidaDeDado() {
    for (int i = 0; i < nomesArquivos.size(); ++i) {
        map<string, string> aux; //= { nomesArquivos[i], "0" };
        buscaRapidaDeDado.push_back(aux);
    }
}

int main(int argc, char* argv[])
{
    auto start = std::chrono::steady_clock::now();
    
    mainDataset.open("dataset_00_sem_virg.csv", fstream::in);

    if (mainDataset.is_open() == false) {
        return -1;
    }
    
    limpaArquivo();
    linhaInicial();
    criarMapComNomeDaColunaAndPosicao();
    
   

    inicializaMatriz_buscaRapidaDeDado();
    while (!fimDoArq)
    {
        NUM_LINHAS_LIDAS = atualizarDataSet();

        #pragma omp parallel
        {

            #pragma omp for nowait
            for (int i = 0; i < nomesArquivos.size(); ++i) {
               pairCodigoDescricao(nomesArquivos[i], i);

            }

            #pragma omp barrier
                   
        }

        escrita_do_dataset(matrizDeDados);

        matrizDeDados.clear();
    }
   


    mainDataset.close();

    auto end = chrono::steady_clock::now();
    std::cout << "Tempo       : " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;    // Ending of parallel region 
}
/*
    Esse map de consulta para saber o nome da coluna e o index que está associado.
    esse map é utilizado na função pairCodigoDescricao()
*/
void linhaInicial() {
    
    finalDataset.open("dataset_00_1000_sem_virg_FINAL.csv", fstream::app);

    string linha;
    
    getline(mainDataset, linha);

    finalDataset << linha;

    finalDataset.close();
    
}

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
    const int NUM_DE_LINHAS_DA_MATRIZ = 10000;

    for (numLinhas = 0; numLinhas < NUM_DE_LINHAS_DA_MATRIZ; numLinhas++) {
        for (int i = 0; i < 25; i++) {
            string dado;
            //RESOLVER BUG DA ÚLTIMA LINHA NAO ADICIONAR '0\n'
            // solução temporia: adicionar = '0\n' na função de escrita do dataset
            if (!getline(mainDataset, dado, ',')) {
                fimDoArq = true;
                cout << "fim do arquivo " << dado << endl;
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
    //GAMBIARRA. ACHAR UMA SOLUÇÃO MAIS LIMPA SEM QUEBRA DE FLUXO: O MOTIVO FOI A FUNÇÃO DE GETLINE NAO ESTAVA PEGANDO O ULTIMO VALOR
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