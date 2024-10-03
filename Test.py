# Databricks notebook source
import requests
import pandas as pd
import json
import os
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# variáveis de conexão
host = 'psql-mock-database-cloud.postgres.database.azure.com'
database = 'ecom1692155331663giqokzaqmuqlogbu'
port = '5432'
user = 'eolowynayhvayxbhluzaqxfp@psql-mock-database-cloud'
password = 'hdzvzutlssuozdonhflhwyjm'

# URL de conexão JDBC
jdbc_url = f'jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}'

# lista todas as tabelas
query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') as t"

# leitura dos dados das tabelas
tables_df = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", query) \
    .load()

# nomes das tabelas
tables_df.show()



# COMMAND ----------

#dbutils.fs.mkdirs('dbfs:/temp/type_ecommerce')

# COMMAND ----------

dbutils.fs.ls('dbfs:/temp/type_ecommerce')

# COMMAND ----------


# leitura dos dados da tabela customers
table_customers = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "customers") \
    .load()

# leitura dos dados da tabela employees
table_employees = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "employees") \
    .load()

# leitura dos dados da tabela offices
table_offices = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "offices") \
    .load()

# leitura dos dados da tabela orderdetails
table_orderdetails= spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "orderdetails") \
    .load()

# leitura dos dados da tabela orders
table_orders = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "orders") \
    .load()

# leitura dos dados da tabela payments
table_payments = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "payments") \
    .load()


# leitura dos dados da tabela product_lines
table_product_lines = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "product_lines") \
    .load()


# leitura dos dados da tabela products
table_products = spark.read.format("jdbc").option("url", jdbc_url) \
    .option("dbtable", "products") \
    .load()

# COMMAND ----------

# tabela customers

table_customers.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_customers")

table_customers.createOrReplaceTempView('tbl_customers')

table_customers.display()


# COMMAND ----------

# tabela employees

table_employees.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_employees")

table_employees.createOrReplaceTempView('tbl_employees')

table_employees.display()

# COMMAND ----------

# tabela offices

table_offices.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_offices")

table_offices.createOrReplaceTempView('tbl_offices')

table_offices.display()

# COMMAND ----------

# tabela orderdetails

table_orderdetails.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_orderdetails")

table_orderdetails.createOrReplaceTempView('tbl_orderdetails')

table_orderdetails.display()

# COMMAND ----------

# tabela orders

table_orders.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_orders")

table_orders.createOrReplaceTempView('tbl_orders')

table_orders.display()

# COMMAND ----------

# tabela payments


table_payments.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_payments")

table_payments.createOrReplaceTempView('tbl_payments')

table_payments.display()

# COMMAND ----------

# product_lines

table_product_lines.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_product_lines")

table_product_lines.createOrReplaceTempView('tbl_product_lines')

table_product_lines.display()


# COMMAND ----------


# tabela products

table_products.write.format("parquet").mode("overwrite").save("dbfs:/temp/type_ecommerce/tbl_products")

table_products.createOrReplaceTempView('tbl_products')

table_products.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --> a.	Qual país possui a maior quantidade de itens cancelados? --> New Zealand
# MAGIC
# MAGIC select 
# MAGIC     c.country,
# MAGIC     count(o.order_number) Qtd_itens_cancelados
# MAGIC from tbl_customers c
# MAGIC left join tbl_orders o on c.customer_number = o.customer_number
# MAGIC where o.status = 'Cancelled'
# MAGIC group by c.country order by 2 desc

# COMMAND ----------

pais_maior_qtd_itens_cancelado = spark.sql("""
                                           
                                           select 
                                                  c.country,
                                                  count(o.order_number) Qtd_itens_cancelados
                                              from tbl_customers c
                                              left join tbl_orders o on c.customer_number = o.customer_number
                                                  where o.status = 'Cancelled'
                                              group by c.country order by 2 desc
                                           
""")

pais_maior_qtd_itens_cancelado.write.format("delta").mode("overwrite").save("dbfs:/temp/type_ecommerce/result_pais_maior_qtd_itens_cancelado")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --> b.	Qual o faturamento da linha de produto mais vendido, considere os pedidos com status 'Shipped', cujo pedido foi realizado no ano de 2005? --> Classic Cars	710,956.92
# MAGIC
# MAGIC select pl.product_line, 
# MAGIC        sum(od.price_each * od.quantity_ordered) as total_revenue
# MAGIC from tbl_orders o
# MAGIC left join tbl_orderdetails od on o.order_number = od.order_number
# MAGIC left join tbl_products p on od.product_code = p.product_code
# MAGIC left join tbl_product_lines pl on p.product_line = pl.product_line
# MAGIC where o.status <> 'Cancelled' and extract(year from o.order_date) = 2005
# MAGIC group by pl.product_line
# MAGIC order by total_revenue desc;

# COMMAND ----------

fat_linha_prod_mais_vend_2025 = spark.sql("""
                                          
                                          select pl.product_line, 
                                                  sum(od.price_each * od.quantity_ordered) as total_revenue
                                          from tbl_orders o
                                          left join tbl_orderdetails od on o.order_number = od.order_number
                                          left join tbl_products p on od.product_code = p.product_code
                                          left join tbl_product_lines pl on p.product_line = pl.product_line
                                                  where o.status <> 'Cancelled' and extract(year from o.order_date) = 2005
                                          group by pl.product_line order by total_revenue desc;
                                          
""")

fat_linha_prod_mais_vend_2025.write.format("delta").mode("overwrite").save("dbfs:/temp/type_ecommerce/result_fat_linha_prod_mais_vend_2025")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --> c.	Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.
# MAGIC
# MAGIC select e.first_name, 
# MAGIC        e.last_name, 
# MAGIC        concat('*****@', substring_index(e.email, '@', -1)) as email
# MAGIC from tbl_employees e
# MAGIC join tbl_offices o on e.office_code = o.office_code
# MAGIC where o.country = 'Japan' or o.office_code = 5;

# COMMAND ----------

dados_vendedor_japao = spark.sql("""
                                 select e.first_name, 
                                        e.last_name, 
                                        concat('*****@', substring_index(e.email, '@', -1)) as email
                                  from tbl_employees e
                                  join tbl_offices o on e.office_code = o.office_code
                                      where o.country = 'Japan' or o.office_code = 5;
                                 
""")

dados_vendedor_japao.write.format("delta").mode("overwrite").save("dbfs:/temp/type_ecommerce/result_dados_vendedor_japao")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Perguntas Teóricas:
# MAGIC
# MAGIC 1. Como você utiliza o Delta Lake no Azure Databricks para garantir a integridade dos dados?
# MAGIC    R: utiliza-se por aplicar boas práticas de MERGE (UPINSERT) entre os dados para cada camada (dependendo do negócio e necessidade) e garantir assim todo o histórico do estado (alterações e etc..) dos dados durante a execução do pipeline.
# MAGIC
# MAGIC
# MAGIC 2. Quais são as vantagens do uso do Spark em comparação com outras tecnologias de processamento de dados?
# MAGIC     R: o uso de paralelismo no processamento dos dados, escalando a nível de memória/processamento a medida da necessidade.
# MAGIC
# MAGIC
# MAGIC 3. Descreva um caso em que você precisou sincronizar dados entre diferentes sistemas.
# MAGIC     R: cenário: são 9
# MAGIC 4. Desenhe uma arquitetura de dados comentada para uma empresa que utiliza Azure e Databricks, incluindo armazenamento, processamento e análise.
# MAGIC     R: A arquitetura tem os seguintes recursos:
# MAGIC         a. Azure Data Factory --> orquestração dos pipelines de dados e é responsável por faze a primeira extração da origem e armazenar os dados na primeira camada do lake
# MAGIC         b. Azure Data Lake ADLS Gen2: com as seguintes camadas: 
# MAGIC             1. raw --> todos os dados são armazenados nessa camada pelo nome do sistema/source que está sendo feita a inegestão - os dados são apagados diariamente
# MAGIC             2. bronze --> camada responsável por armazenar os dados pelo nome do sistema/source e mantendo a hierarquia por ano/mês/dia
# MAGIC             3. silver --> responsável por armazenar as tabelas de domínio com os dados mais atualizados e já com todas as transformações
# MAGIC             4. gold --> camada responsável por armazenar as tabelas de dimnensão e fatos, sendo organizadas por área / assunto
# MAGIC         c. Azure Databricks: responsável por todo processamento de dados com as transformaçções dos dados garantindo que os dados passem em todas as camadas
# MAGIC         d. Power BI Pro: responsável por conectar no endpoint do Azure Databricks (arquitetura Lake Warehouse) onde contém todas as dimensões e fatos
# MAGIC
# MAGIC 5. Como você garante a escalabilidade e a robustez da arquitetura de dados?
# MAGIC     R: criando uma arquitetura aderente à necessidade de negócio contendo as boas práticas e uma metodologia, ou seja, Lake Warehouse
# MAGIC
# MAGIC 6. Como você implementa a criptografia de dados em repouso e em trânsito?
# MAGIC     R: Nunca apliquei criptografia de dados em repouso ou em trânsito com Databricks, apenas com Synapse e em repouso (ele utiliza uma feature igual ao do SQL Server)
# MAGIC
# MAGIC 7. Como você gerencia a qualidade dos dados em um pipeline de dados?
# MAGIC     R: garantindo que os dados na camada Silver tenham o mesmo data type (ou compatíveis) aos dados de origem e garantindo que os meus dados na camada Silver sejam exatamente uma réplica do meu database de origem
# MAGIC
# MAGIC 8. Qual a importância do FinOps para a engenharia de dados?
# MAGIC     R: não sei, ainda não tive a oportunidade de entender os detalhes e sua aplicabilidade
# MAGIC
# MAGIC 9. Como o DevOps ajuda o engenheiro de dados?
# MAGIC     R: por garantir que vários engenheiros trabalhem em várias partes de um ou mais projetos e possam unificar seus códigos de diversas branchs para as branch de Dev, QA e Prd
# MAGIC
# MAGIC 10. Como iniciamos um projeto de pipeline de dados?
# MAGIC     R: primeiramente identificando as necessidades das áreas e entendendo quais perguntas (análises) os dados devem responder e o impacto da tomada de decisão para as áreas. A partir dessas necessidades e suas viabilidades, é possível definir uma arquitetura escalável e as necessidades dos recursos tecnológicos
# MAGIC
# MAGIC 11. Como realizar CI/CD em um pipeline de dados?
# MAGIC     R: depois dos códigos testados na branch do engenheiro, cria-se uma PR para Dev e nesse ambiente é possível realizar outros testes e em seguida cria-se um novo PR (Pull Request) de Dev para QA. Em QA, a equipe técnica de analistas podem validar o processo com os dados de amostra e por fil cria-se uma novo PR para QA para Prd
# MAGIC
# MAGIC 12. Quais ferramentas de orquestração você já trabalhou?
# MAGIC     R: Iniciei com as seguintes:
# MAGIC         a. Talend Data Integrator
# MAGIC         b. SQL Server Integration Services - SSIS
# MAGIC         c. Informatica Power Center
# MAGIC         d. IBM InfoSphere DataStage
# MAGIC         e. Dremio
# MAGIC         f. Apache Airflow
# MAGIC         g. Azure Data Factory
# MAGIC         h. AWS Glue Studio
# MAGIC
# MAGIC
# MAGIC 13. Quais suas motivações para ser um engenheiro de dados?
# MAGIC     R: pode criar as mais diversas soluções para ingestão e integração de dados; criar soluções de integração de dados com python, pyspark, sparksql e sql; o desafio de criar fluxos de dados eficientes, reutilizavéis e sempre com as boas práticas
# MAGIC

# COMMAND ----------


