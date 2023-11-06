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
vector<vector<string>> matrizDeDados;
map<string, int> idxColuna;
bool fimDoArq;
vector<string> nomesArquivos;
int NUM_LINHAS_LIDAS;

void criarMapComNomeDaColunaAndPosicao();

int atualizarDataSet();

void escrita_do_dataset(vector<vector<string>> escritaMatriz);

void pairCodigoDescricao(string nomeArquivo);

void linhaInicial();

void limpaArquivo() {
    for (int i = 0; i < nomesArquivos.size(); ++i) {
        fstream arq;
        arq.open(nomesArquivos[i], fstream::out);
       // arq.clear();
        arq.close();
    }
};


int main(int argc, char* argv[])
{
   
    
    auto start = std::chrono::steady_clock::now();

    nomesArquivos = { "cdtup.csv", "berco.csv", "portoatracacao.csv", "mes.csv", "tipooperacao.csv",
        "tiponavegacaoatracacao.csv", "terminal.csv", "origem.csv", "destino.csv", "naturezacarga.csv", "sentido.csv" };
    limpaArquivo();
    mainDataset.open("dataset_00_sem_virg.csv", fstream::in);
    
    if (mainDataset.is_open() == false) {
        return -1;
    }
   
    
    linhaInicial();
    criarMapComNomeDaColunaAndPosicao();
    fimDoArq = false;
    NUM_LINHAS_LIDAS = 0;

    while (!fimDoArq)
    {
        NUM_LINHAS_LIDAS = atualizarDataSet();
       
        #pragma omp parallel
        {

            #pragma omp for nowait
            for (int i = 0; i < nomesArquivos.size(); ++i) {
               pairCodigoDescricao(nomesArquivos[i]);

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
    const int NUM_DE_LINHAS_DA_MATRIZ = 500;

    for (numLinhas = 0; numLinhas < NUM_DE_LINHAS_DA_MATRIZ; numLinhas++) {
        for (int i = 0; i < 25; i++) {
            string dado;
            //RESOLVER BUG DA ÚLTIMA LINHA NAO ADICIONAR '0\n'
            // solução temporia: adicionar = '0\n' na função de escrita do dataset
            if (!getline(mainDataset, dado, ',')) {
                fimDoArq = true;
                //cout << "fim do arquivo " << dado << endl;
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


void pairCodigoDescricao(string nomeArquivo) {

    
    int NUM_COLUM = idxColuna[nomeArquivo];
  
    for (int i = 0; i < NUM_LINHAS_LIDAS; i++) {
        std::fstream arquivo;
        arquivo.open(nomeArquivo, fstream::in);
        string dado = matrizDeDados[i][NUM_COLUM];
        string linha;
        string id;
        string valor;
        int countID = 1;
        bool encontrouNoDicionario = true;
            
        while (getline(arquivo, linha)) {
            countID++;
            int posVirgula = linha.find(',');
            int posFinal = linha.size();
            id = linha.substr(0, posVirgula);
            valor = linha.substr(posVirgula+1, posFinal);
            if (valor.compare(dado) == 0) {
                matrizDeDados[i][NUM_COLUM] = id;
                encontrouNoDicionario = false;
                break;
            }
            
        }
        if (encontrouNoDicionario == true) {
            arquivo.close();
            arquivo.open(nomeArquivo, fstream::app);
            id = to_string(countID);
            matrizDeDados[i][NUM_COLUM] = id;
            arquivo << id << "," << dado << endl;
            arquivo.seekg(0);
        }
        arquivo.close();
    }
   
}