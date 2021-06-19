from ftplib import FTP
import py7zr
import pandas as pd
from pyspark.sql import *
from pyspark.sql.types import *
import pyarrow.parquet as pq
import pyarrow.csv as pc


filename = 'ES2015.7z'
with FTP('ftp.mtps.gov.br') as ftp:
    ftp.login()
    ftp.cwd('pdet/microdados/RAIS/2015')
    try:
        ftp.retrbinary("RETR " + filename, open(filename, 'wb').write)
        ftp.quit()
        print('download realizado',filename)
        try:
            with py7zr.SevenZipFile(filename, mode='r') as z:
                z.extractall()
                print('extração realizado',filename)
        except:
            print('falha ao extrair o arquivo')
    except:
        print('falha no download')
print('arquivo baixado e extraido com sucesso!!!')

try:
    dfsep = pd.read_fwf('ES2015.txt', sep=';')
except:
    print('erro ao importar txt')

try:
    dfsep.to_csv('ES2015.csv', index=False)
except:
    print('erro ao converter para csv')

spark = SparkSession.builder.appName("demo-app").getOrCreate()

try:
    csvFile = spark.read.csv('ES2015.csv', header=True, sep=';', inferSchema=True)
except:
    print('erro ao ler csv')
    
try:
    df = csvFile.toDF(*(c.replace(',', '_') for c in csvFile.columns))
    df = df.toDF(*(c.replace(' ', '_') for c in df.columns))
    df = df.toDF(*(c.replace('(', '') for c in df.columns))
    df = df.toDF(*(c.replace(')', '') for c in df.columns))
    df = df.toDF(*(c.replace('.', '') for c in df.columns))
    df = df.withColumnRenamed("CBO_Ocupa��o_2002", "cbo")
    df = df.withColumnRenamed("Faixa_Remun_M�dia_SM", "media_salario")
except:
    print('erro ao formatar colunas')
    

try:
    df.createOrReplaceTempView("mediasalario")
except:
    print('erro ao criar arquivo temporario')

try:
    rel = spark.sql("SELECT SUBSTR(AVG(media_salario),1 ,1) as media_salario FROM mediasalario where SUBSTR(cbo, 1, 3) = 212 and SUBSTR(Sexo_Trabalhador, 5, 2) = '02' UNION ALL SELECT SUBSTR(AVG(media_salario),1 ,1) as media_mulher FROM mediasalario where SUBSTR(cbo, 1, 3) = 212 and SUBSTR(Sexo_Trabalhador, 5, 2) = '01' ")
    relarq = spark.createDataFrame(rel.collect())
    relarq.write.format('csv').option('header',True).mode('overwrite').option('sep','|').save('rel.csv')
    print('arquivo criado')
    rel.show()
    
except:
    print('erro na geracao do relatorio')