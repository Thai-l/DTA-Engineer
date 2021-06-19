A3 Data Challenge - Hackathon de Engenharia de Dados

Um pouco da curva de aprendizagem e mudanças de caminhos.

Iniciamos o projeto com a ideia de analisar os dados pelo pandas. Após algumas tentativas frustadas em analisar um volume maior de dados, percebemos que não seria possível. Então resolver utilizar o pyspark por ser uma biblioteca do Apache Spark e pela possibilidade de escrever comandos SQL.

Durante o hackathon, tivemos o nosso primeiro contato com o Apache Spark, e ficamos encantado como ele otimiza o processamento distribuindo em várias tarefas que ocorrem de forma simultânea nos node que são gerenciado por um node master.

Após a fantástica mentoria que tivemos com o Neylson, ele comentou sobre alguns serviços da GCP, o que nos fez estudar como implementar o Spark no DATAPROC. Infelizmente não conseguimos fazer o job funcionar dentro no cluster que criamos.

O código jopspark.py foi desenvolvido para baixar os dados do ftp. Fazer o tratamento dos dados e gerar o resultado em formato csv para uso em ferramentas de visualização (Power BI) ou Apache Superset. Porém tivemos dificuldade em acessar o Cloud Storage.

Instalamos, configuramos e criamos algumas DAGs com o Airflow on-premises para entender o seu funcionamento, e posteriormente implementar no GCP, porém não foi possível concluir.

O Nosso time agradece a A3 Data pelas orientações e a oportunidade de realizar uma tarefa que talvez não seria possível executar no mundo acadêmico.

O nosso objetivo é finalizar todas as ideias citadas acima para que o projeto funcione e responda todas as perguntas do desafio A3Data Challenge.  
