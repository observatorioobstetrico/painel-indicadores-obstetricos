library(dplyr)
library(janitor)
library(tidyr)
library(data.table)
library(getPass)
library(repr)
library(httr)
library(microdatasus)

# Lendo um dataframe com as informações dos municípios --------------------
df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  mutate_if(is.numeric, as.character)

variaveis_sinasc <- c(
  "ORIGEM", "CODESTAB", "CODMUNNASC", "LOCNASC", "IDADEMAE", "ESTCIVMAE", "ESCMAE", 
  "CODOCUPMAE", "QTDFILVIVO", "QTDFILMORT", "CODMUNRES", "GESTACAO", "GRAVIDEZ",
  "PARTO", "CONSULTAS", "DTNASC", "HORANASC", "SEXO", "APGAR1", "APGAR5", "RACACOR",
  "PESO", "IDANOMAL", "DTCADASTRO", "CODANOMAL", "NUMEROLOTE", "VERSAOSIST", 
  "DTRECEBIM", "DIFDATA", "DTRECORIGA", "NATURALMAE", "CODMUNNATU", "CODUFNATU", 
  "ESCMAE2010", "SERIESCMAE", "DTNASCMAE", "RACACORMAE", "QTDGESTANT", "QTDPARTNOR",
  "QTDPARTCES", "IDADEPAI", "DTULTMENST", "SEMAGESTAC", "TPMETESTIM", "CONSPRENAT",
  "MESPRENAT", "TPAPRESENT", "STTRABPART", "STCESPARTO", "TPNASCASSI", "TPFUNCRESP",
  "TPDOCRESP", "DTDECLARAC", "ESCMAEAGR1", "STDNEPIDEM", "STDNNOVA", "CODPAISRES", 
  "TPROBSON", "PARIDADE", "KOTELCHUCK", "CONTADOR"
)


# Baixando os dados preliminares de 2023, 2024 e 2025 do SINASC ------------
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2023_csv.zip", "R/databases/SINASC_2023_csv.zip", mode = "wb")
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2024_csv.zip", "R/databases/SINASC_2024_csv.zip", mode = "wb")
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_2025_csv.zip", "R/databases/SINASC_2025_csv.zip", mode = "wb")

## Lendo os dados preliminares e excluindo os arquivos baixados
dados_preliminares_2023_aux1 <- fread("R/databases/SINASC_2023_csv.zip", sep = ";") |>
  as.data.frame()
file.remove("R/databases/SINASC_2023_csv.zip")

dados_preliminares_2024_aux1 <- fread("R/databases/SINASC_2024_csv.zip", sep = ";") |>
  as.data.frame()
file.remove("R/databases/SINASC_2024_csv.zip")

dados_preliminares_2025_aux1 <- fread("R/databases/SINASC_2025_csv.zip", sep = ";") |>
  as.data.frame()
file.remove("R/databases/SINASC_2025_csv.zip")

### Criando as variáveis que não existem e as preenchendo com NA, se for o caso
dados_preliminares_2023_aux1[setdiff(variaveis_sinasc, names(dados_preliminares_2023_aux1))] <- NA
dados_preliminares_2024_aux1[setdiff(variaveis_sinasc, names(dados_preliminares_2024_aux1))] <- NA
dados_preliminares_2025_aux1[setdiff(variaveis_sinasc, names(dados_preliminares_2025_aux1))] <- NA

## Checando se as colunas são as mesmas
names(dados_preliminares_2023_aux1)[which(!(names(dados_preliminares_2023_aux1) %in% names(dados_preliminares_2024_aux1)))]
names(dados_preliminares_2024_aux1)[which(!(names(dados_preliminares_2024_aux1) %in% names(dados_preliminares_2023_aux1)))]
names(dados_preliminares_2025_aux1)[which(!(names(dados_preliminares_2025_aux1) %in% names(dados_preliminares_2023_aux1)))]

## Retirando as variáveis que não batem
dados_preliminares_2023_aux2 <- dados_preliminares_2023_aux1 |>
  select(!c(CONTADOR)) |>
  mutate_if(is.numeric, as.character)

dados_preliminares_2024_aux2 <- dados_preliminares_2024_aux1 |>
  mutate_if(is.numeric, as.character)

dados_preliminares_2025_aux2 <- dados_preliminares_2025_aux1 |>
  mutate_if(is.numeric, as.character)

rm(dados_preliminares_2023_aux1, dados_preliminares_2024_aux1, dados_preliminares_2025_aux1)

## Juntando as três bases
dados_preliminares_aux <- full_join(dados_preliminares_2023_aux2, dados_preliminares_2024_aux2) |>
  full_join(dados_preliminares_2025_aux2)

rm(dados_preliminares_2023_aux2, dados_preliminares_2024_aux2, dados_preliminares_2025_aux2)

## Transformando algumas variáveis
dados_preliminares <- left_join(dados_preliminares_aux, df_aux_municipios) |>
  select(!CODMUNRES) |>
  rename(CODMUNRES = res_codigo_adotado) |>
  mutate(
    ano_nasc = as.numeric(substr(DTNASC, nchar(DTNASC) - 3, nchar(DTNASC))), 
    .after = DTNASC, 
    .keep = "unused"
  ) |>
  mutate_at(
    vars(CODMUNRES, GESTACAO, GRAVIDEZ, PARTO, CONSULTAS, SEXO, IDANOMAL, RACACORMAE, TPROBSON, STCESPARTO, STTRABPART),
    as.numeric
  ) |>
  mutate(
    GESTACAO = ifelse(is.na(GESTACAO), 9, GESTACAO),
    PARTO = ifelse(is.na(PARTO), 9, PARTO),
    CONSULTAS = ifelse(is.na(CONSULTAS), 9, CONSULTAS),
    GRAVIDEZ = ifelse(is.na(GRAVIDEZ), 9, GRAVIDEZ)
  )


# Filtrando e salvando as bases específicas --------------------------------
## Número de nascidos vivos ------------------------------------------------
df_nascidos_consolidados <- read.csv2("R/databases/Nascimentos_muni_consolidados.csv")

df_nascidos_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_nascidos_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')

df_nascidos_completo <- full_join(df_nascidos_consolidados, df_nascidos_aux) |>
  arrange(UF, Municipio)

### Exportando os dados 
write.table(df_nascidos_completo, 'R/databases/Nascimentos_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Nascidos vivos prematuros -----------------------------------------------
df_prematuros_consolidados <- read.csv2("R/databases/Prematuridade_muni_consolidados.csv")

df_prematuros_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuros_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')

sort(unique(df_prematuros_consolidados$Gestacao), na.last = TRUE)
sort(unique(df_prematuros_aux$Gestacao), na.last = TRUE)

df_prematuros_completo <- full_join(df_prematuros_consolidados, df_prematuros_aux)

### Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(df_prematuros_completo, 'R/databases/Prematuridade_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Tipo de gestação --------------------------------------------------------
df_tipo_gravidez_consolidados <- read.csv2("R/databases/Tipo_gravidez_muni_consolidados.csv")

df_tipo_gravidez_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_tipo_gravidez_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gravidez', 'Nascidos')

sort(unique(df_tipo_gravidez_consolidados$Gravidez), na.last = TRUE)
sort(unique(df_tipo_gravidez_aux$Gravidez), na.last = TRUE)

df_tipo_gravidez_completo <- full_join(df_tipo_gravidez_consolidados, df_tipo_gravidez_aux)

### Exportando os dados 
write.table(df_tipo_gravidez_completo, 'R/databases/Tipo_gravidez_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Tipo de parto -----------------------------------------------------------
df_tipo_parto_consolidados <- read.csv2("R/databases/Tipo_parto_muni_consolidados.csv")

df_tipo_parto_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_tipo_parto_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Parto', 'Nascidos')

sort(unique(df_tipo_parto_consolidados$Parto), na.last = TRUE)
sort(unique(df_tipo_parto_aux$Parto), na.last = TRUE)

df_tipo_parto_completo <- full_join(df_tipo_parto_consolidados, df_tipo_parto_aux)

### Exportando os dados 
write.table(df_tipo_parto_completo, 'R/databases/Tipo_parto_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Número de consultas de pré-natal ----------------------------------------
df_consultas_consolidados <- read.csv2("R/databases/Consultas_PreNatal_muni_consolidados.csv")

df_consultas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Consultas', 'Nascidos')

sort(unique(df_consultas_consolidados$Consultas), na.last = TRUE)
sort(unique(df_consultas_aux$Consultas), na.last = TRUE)

df_consultas_completo <- full_join(df_consultas_consolidados, df_consultas_aux)

### Exportando os dados 
write.table(df_consultas_completo, 'R/databases/Consultas_PreNatal_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Sexo fetal --------------------------------------------------------------
df_sexo_fetal_consolidados <- read.csv2("R/databases/Sexo_fetal_muni_consolidados.csv")

df_sexo_fetal_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_sexo_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Sexo', 'Nascidos')

sort(unique(df_sexo_fetal_consolidados$Sexo), na.last = TRUE)
sort(unique(df_sexo_fetal_aux$Sexo), na.last = TRUE)

df_sexo_fetal_completo <- full_join(df_sexo_fetal_consolidados, df_sexo_fetal_aux)

### Exportando os dados 
write.table(df_sexo_fetal_completo, 'R/databases/Sexo_fetal_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Apgar 1 -----------------------------------------------------------------
df_apgar1_consolidados <- read.csv2("R/databases/Apgar1_muni_consolidados.csv")

df_apgar1_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_apgar1_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar1', 'Nascidos')

sort(unique(df_apgar1_consolidados$Apgar1), na.last = TRUE)
sort(unique(df_apgar1_aux$Apgar1), na.last = TRUE)

df_apgar1_completo <- full_join(df_apgar1_consolidados, df_apgar1_aux)

### Exportando os dados 
write.table(df_apgar1_completo, 'R/databases/Apgar1_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Apgar 5 -----------------------------------------------------------------
df_apgar5_consolidados <- read.csv2("R/databases/Apgar5_muni_consolidados.csv")

df_apgar5_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_apgar5_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar5', 'Nascidos')

sort(unique(df_apgar5_consolidados$Apgar5), na.last = TRUE)
sort(unique(df_apgar5_aux$Apgar5), na.last = TRUE)

df_apgar5_completo <- full_join(df_apgar5_consolidados, df_apgar5_aux)

### Exportando os dados 
write.table(df_apgar5_completo, 'R/databases/Apgar5_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Anomalias ---------------------------------------------------------------
df_anomalias_consolidados <- read.csv2("R/databases/Anomalias_muni_consolidados.csv")

df_anomalias_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_anomalias_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Anomalia', 'Nascidos')

sort(unique(df_anomalias_consolidados$Anomalia), na.last = TRUE)
sort(unique(df_anomalias_aux$Anomalia), na.last = TRUE)

df_anomalias_completo <- full_join(df_anomalias_consolidados, df_anomalias_aux)

### Exportando os dados 
write.table(df_anomalias_completo, 'R/databases/Anomalias_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Peso fetal menor que 2500g ----------------------------------------------
df_peso_fetal_consolidados <- read.csv2("R/databases/Peso_menor_2500_muni_consolidados.csv")

df_peso_fetal_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PESO) |>
  filter(PESO < 2500) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_peso_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')

df_peso_fetal_completo <- full_join(df_peso_fetal_consolidados, df_peso_fetal_aux)

### Exportando os dados 
write.table(df_peso_fetal_completo, 'R/databases/Peso_menor_2500_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Raça/cor da mãe ---------------------------------------------------------
df_racamae_consolidados <- read.csv2("R/databases/Raca_mae_muni_consolidados.csv")

df_racamae_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_racamae_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Raca_mae', 'Nascidos')

sort(unique(df_racamae_consolidados$Raca_mae), na.last = TRUE)
sort(unique(df_racamae_aux$Raca_mae), na.last = TRUE)

df_racamae_completo <- full_join(df_racamae_consolidados, df_racamae_aux)

### Exportando os dados 
write.table(df_racamae_completo, 'R/databases/Raca_mae_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Robson ------------------------------------------------------------------
df_robson_consolidados <- read.csv2("R/databases/Robson_muni_consolidados.csv")

df_robson_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_robson_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Nascidos')

sort(unique(df_robson_consolidados$Robson), na.last = TRUE)
sort(unique(df_robson_aux$Robson), na.last = TRUE)

df_robson_completo <- full_join(df_robson_consolidados, df_robson_aux)

### Exportando os dados 
write.table(df_robson_completo, 'R/databases/Robson_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Parto prematuro ---------------------------------------------------------
df_prematuro_pcdas_consolidados <- read.csv2("R/databases/Prematuro_PCDAS_muni_consolidados.csv")

df_prematuro_pcdas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuro_pcdas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')

sort(unique(df_prematuro_pcdas_consolidados$Gestacao), na.last = TRUE)
sort(unique(df_prematuro_pcdas_aux$Gestacao), na.last = TRUE)

df_prematuro_pcdas_completo <- full_join(df_prematuro_pcdas_consolidados, df_prematuro_pcdas_aux)

### Exportando os dados 
write.table(df_prematuro_pcdas_completo, 'R/databases/Prematuro_PCDAS_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Cesárea eletiva ---------------------------------------------------------
df_ces_consolidados <- read.csv2("R/databases/Cesaria_antes_do_parto_muni_consolidados.csv")

df_ces_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
  filter(TPROBSON %in% c(2, 4)) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_ces_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'cesarea_antes_do_parto', 'Nascidos')

sort(unique(df_ces_consolidados$cesarea_antes_do_parto), na.last = TRUE)
sort(unique(df_ces_aux$cesarea_antes_do_parto), na.last = TRUE)

df_ces_completo <- full_join(df_ces_consolidados, df_ces_aux)

### Exportando os dados
write.table(df_ces_completo, 'R/databases/Cesaria_antes_do_parto_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Partos induzidos --------------------------------------------------------
df_parto_induzido_consolidados <- read.csv2("R/databases/Parto_induzido_muni_consolidados.csv")

df_parto_induzido_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
  filter(TPROBSON %in% c(2, 4)) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_parto_induzido_aux) <- c('UF','Municipio', 'Codigo', 'Ano', 'Robson', 'parto_induzido', 'Nascidos')

sort(unique(df_parto_induzido_consolidados$parto_induzido), na.last = TRUE)
sort(unique(df_parto_induzido_aux$parto_induzido), na.last = TRUE)

df_parto_induzido_completo <- full_join(df_parto_induzido_consolidados, df_parto_induzido_aux)

### Exportando os dados
write.table(df_parto_induzido_completo, 'R/databases/Parto_induzido_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Prematuridade consultas -------------------------------------------------
df_prematuridade_consultas_consolidados <- read.csv2("R/databases/Prematuridade_consultas_muni_consolidados.csv")

df_prematuridade_consultas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuridade_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Consultas', 'Nascidos')

sort(unique(df_prematuridade_consultas_consolidados$Gestacao), na.last = TRUE)
sort(unique(df_prematuridade_consultas_aux$Gestacao), na.last = TRUE)

sort(unique(df_prematuridade_consultas_consolidados$Consultas), na.last = TRUE)
sort(unique(df_prematuridade_consultas_aux$Consultas), na.last = TRUE)

df_prematuridade_consultas_completo <- full_join(df_prematuridade_consultas_consolidados, df_prematuridade_consultas_aux)

### Exportando os dados 
write.table(df_prematuridade_consultas_completo, 'R/databases/Prematuridade_consultas_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)


## Prematuros, cesáreas e Robson -------------------------------------------
df_robson_cesar_consolidados <- read.csv2("R/databases/Robson_cesar_muni_consolidados.csv")

df_robson_cesar_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_robson_cesar_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Parto', 'Nascidos')

df_robson_cesar_completo <- full_join(df_robson_cesar_consolidados, df_robson_cesar_aux)

### Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(df_robson_cesar_completo, 'R/databases/Robson_cesar_muni2025.csv', sep = ";", dec = ".", row.names = FALSE)

