# Account Balance Calculation Project

## Descrição
Este projeto implementa um sistema de cálculo de saldo de conta corrente usando PySpark. Ele processa movimentações diárias, calcula saldos acumulados e gera um relatório final com os saldos atualizados para todos os clientes por data.

## Ambiente de Desenvolvimento
- Windows Subsystem for Linux (WSL) rodando Ubuntu
- Python 3.11.9
- PySpark 3.5.3
- Jupyter 1.1.1
- loguro 0.7.2 

## Pré-requisitos
Certifique-se de ter os seguintes componentes instalados no seu ambiente WSL Ubuntu:

1. Python 3.11.9
2. pip (gerenciador de pacotes do Python)
3. Java Development Kit (JDK)

## Instalação

1. Clone o repositório (ou baixe os arquivos do projeto):
   ```
   git clone [URL_DO_REPOSITORIO]
   cd [NOME_DO_DIRETORIO]
   ```

2. Crie um ambiente virtual (opcional, mas recomendado):
   ```
   python3.11 -m venv venv
   source venv/bin/activate
   ```

3. Instale as dependências:
   ```
   pip install pyspark==3.5.3 jupyter==1.1.1 loguru==0.7.2
   ```

## Estrutura do Projeto
```
project/
│
├── data/
│   ├── tabela_saldo_inicial.txt
│   ├── movimentacao_dia_02_04_2022.txt
│   └── movimentacao_dia_03_04_2022.txt
│
├── src/
│   └── account_balance_calculation.py
│   └── logger.py
│
├── notebooks/
│   └── account_balance_analysis.ipynb
│
├── README.md
```

## Uso

1. Navegue até o diretório do projeto no WSL.

2. Ative o ambiente virtual (se estiver usando):
   ```
   source venv/bin/activate
   ```

3. Para executar o script Python e gerar o relatório de saldos atualizados salvos em arquivo CSV no diretório data:
   ```
   python main.py
   ```

3. Para abrir o Jupyter Notebook:
   ```
   jupyter notebook
   ```
   Navegue até `notebooks/account_balance_analysis.ipynb` na interface do Jupyter.

## Arquivos de Entrada
- `tabela_saldo_inicial.txt`: Contém os saldos iniciais dos clientes.
- `movimentacao_dia_02_04_2022.txt`: Movimentações do dia 02/04/2022.
- `movimentacao_dia_03_04_2022.txt`: Movimentações do dia 03/04/2022.

Certifique-se de que estes arquivos estejam no diretório `data/` antes de executar o script.

## Saída
O script gera um DataFrame PySpark com os saldos atualizados de todos os clientes por data. O resultado é exibido no console e pode ser facilmente exportado para diversos formatos (CSV, Parquet, etc.) se necessário.

## Resolução de Problemas

1. Se encontrar problemas relacionados à versão do Python, verifique se está usando Python 3.11.9:
   ```
   python --version
   ```

2. Para problemas com o PySpark, verifique se a variável de ambiente JAVA_HOME está configurada corretamente:
   ```
   echo $JAVA_HOME
   ```

3. Se o Jupyter Notebook não abrir, verifique se está instalado corretamente:
   ```
   jupyter --version
   ```

## Contribuição
Contribuições para este projeto são bem-vindas. Por favor, abra uma issue para discutir mudanças propostas ou envie um pull request com suas melhorias.

## Contato
Para mais informações sobre este projeto, entre em contato comigo em [meu e-mail](mailto:bruoli3@gmail.com).