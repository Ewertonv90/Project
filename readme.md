Código de teste técnico


Requisitos na máquina/Pod/Container/env:

- Python 3.10.x a 3.11.x
- SQLite ( qualquer versão)
- Pyarrow
- Pandas

Para executar:

Para Windows:
    - Navegar até o diretorio principal /project
    - criar o ambiente virtual: py -3.11 -m venv venv
    - entrar no venv para ativar :  "cd project/venv/" e depois "venv\Scripts\activate"
    - instalar as dependencias: "pip install -r requirements.txt"
    - executar o main.py :  "python src/main.py"

Para Linux:
    - Navegar até o diretorio principal /project
    - criar o ambiente virtual: py -3.11 -m venv venv
    - entrar no venv para ativar :  "cd project/venv/" e depois "source venv/bin/activate"
    - instalar as dependencias: "pip install -r requirements.txt"
    - instalar bibliotecas extras: 
       - pip install --upgrade pip && pip install --upgrade requests urllib3 chardet
    - executar o main.py :  "python src/main.py"

resumo de execução: 

cd project
py -3.11 -m venv venv
venv\Scripts\activate  
<!-- Se não funcioanr no windows tem que desbloquearas politicas de exeução com:  "Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
" RemoteSigned -->
<!-- Linux/Mac : source venv/bin/activate -->
pip install -r requirements.txt
python src/main.py


caso tenha problemas de versão ou cache do python faça um purge:

>> pip cache purge
>> pip install numpy
>> pip install pandas
