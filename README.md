# Nome do Projeto

Breve descrição do projeto aqui.

## Requisitos de Sistema

- Compilador C++ compatível com OpenMP (por exemplo, GCC versão 4.2 ou superior)
- Sistema operacional (ex.: Linux, macOS, Windows)

## Configuração do Ambiente

1. Instale o GCC (se ainda não estiver instalado):

Para Debian/Ubuntu baseado em sistemas Linux, você pode usar:

```bash
$ sudo apt-get install g++
```

Para sistemas baseados em Red Hat:

```bash
$ sudo yum install gcc-c++
```

Para macOS, você pode instalar o GCC através do Homebrew:

```bash
$ brew install gcc
```

Para Windows, você pode usar o MinGW ou o Microsoft Visual C++ com suporte a OpenMP.

## Configuração do Ambiente

Para compilar o código, execute o seguinte comando no console de comandos: 

```
$ g++ -fopenmp -o categorization_with_threads main.cpp
```

Este comando compila o código-fonte main.cpp e cria um executável.

## Executando

Após compilar o programa, você pode executá-lo diretamente a partir do terminal:

```
$ ./categorization_with_threads
```

Em Windowns pode ser necessário colocar a extensão `.exe` no final do arquivo para executar.

## Variáveis de Ambiente (Opcional)

Para controlar o número de threads usadas pelo OpenMP, você pode definir a variável de ambiente OMP_NUM_THREADS antes de executar o seu programa:

```
$ export OMP_NUM_THREADS=4
$ ./categorization_with_threads
```