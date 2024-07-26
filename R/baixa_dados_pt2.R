library(dplyr)
library(janitor)
library(tidyr)
library(data.table)
library(getPass)
library(repr)
library(httr)
library(microdatasus)

token = getPass()  #Token de acesso à API da PCDaS

url_base = "https://bigdata-api.fiocruz.br"

convertRequestToDF <- function(request){
  variables = unlist(content(request)$columns)
  variables = variables[names(variables) == "name"]
  column_names <- unname(variables)
  values = content(request)$rows
  df <- as.data.frame(do.call(rbind,lapply(values,function(r) {
    row <- r
    row[sapply(row, is.null)] <- NA
    rbind(unlist(row))
  } )))
  names(df) <- column_names
  return(df)
}

endpoint <- paste0(url_base,"/","sql_query")

# Criando data.frames auxiliares ------------------------------------------
## Obtendo um dataframe com as regiões, UFs e nomes de cada município -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_codigo_adotado, res_MUNNOMEX, res_SIGLA_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sinasc\\"',
                ' GROUP BY res_codigo_adotado, res_MUNNOMEX, res_SIGLA_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("CODMUNRES", "res_MUNNOMEX", "res_SIGLA_UF", "res_REGIAO", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(CODMUNRES)


# Baixando os dados preliminares de 2023 e 2024 do SINASC ------------------
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/DNOPEN23.csv", "R/databases/DNOPEN23.csv", mode = "wb")
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/DNOPEN24.csv", "R/databases/DNOPEN24.csv", mode = "wb")

dados_preliminares_2022_aux1 <- fetch_datasus(
  year_start = 2022, 
  year_end = 2023,
  information_system = "SINASC"
) 

## Por enquanto, o microdatasus só tem dados até 2022
unique(substr(dados_preliminares_2022_aux1$DTNASC, nchar(dados_preliminares_2022_aux1$DTNASC) - 3, nchar(dados_preliminares_2022_aux1$DTNASC)))

## Lendo os dados preliminares e excluindo os arquivos baixados
dados_preliminares_2023_aux1 <- read.csv2("R/databases/DNOPEN23.csv")
file.remove("R/databases/DNOPEN23.csv")

dados_preliminares_2024_aux1 <- read.csv2("R/databases/DNOPEN24.csv")
file.remove("R/databases/DNOPEN24.csv")

## Checando se as colunas são as mesmas
names(dados_preliminares_2023_aux1)[which(!(names(dados_preliminares_2023_aux1) %in% names(dados_preliminares_2022_aux1)))]
names(dados_preliminares_2024_aux1)[which(!(names(dados_preliminares_2024_aux1) %in% names(dados_preliminares_2022_aux1)))]

names(dados_preliminares_2022_aux1)[which(!(names(dados_preliminares_2022_aux1) %in% names(dados_preliminares_2023_aux1)))]
names(dados_preliminares_2022_aux1)[which(!(names(dados_preliminares_2022_aux1) %in% names(dados_preliminares_2024_aux1)))]

## Retirando as variáveis que não batem
dados_preliminares_2022_aux2 <- dados_preliminares_2022_aux1 |>
  select(!CONTADOR) 

dados_preliminares_2023_aux2 <- dados_preliminares_2023_aux1 |>
  select(!c(contador, OPORT_DN)) |>
  mutate_if(is.numeric, as.character)

dados_preliminares_2024_aux2 <- dados_preliminares_2024_aux1 |>
  select(!c(contador, OPORT_DN)) |>
  mutate_if(is.numeric, as.character)

## Juntando as três bases
dados_preliminares_aux <- full_join(
  dados_preliminares_2022_aux2, 
  full_join(dados_preliminares_2023_aux2, dados_preliminares_2024_aux2)
)

## Transformando algumas variáveis
dados_preliminares <- left_join(dados_preliminares_aux, df_aux_municipios) |>
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


# Número de nascidos vivos ------------------------------------------------
df_nascidos_2021 <- read.csv2("R/databases/Nascimentos_muni2021.csv")

df_nascidos_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_nascidos_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')

df_nascidos_completo <- full_join(df_nascidos_2021, df_nascidos_aux) |>
  arrange(UF, Municipio)

# Exportando os dados 
write.table(df_nascidos_completo, 'R/databases/Nascimentos_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Nascidos vivos prematuros -----------------------------------------------
df_prematuros_2021 <- read.csv2("R/databases/Prematuridade_muni2021.csv")

df_prematuros_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuros_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')

sort(unique(df_prematuros_2021$Gestacao), na.last = TRUE)
sort(unique(df_prematuros_aux$Gestacao), na.last = TRUE)

df_prematuros_completo <- full_join(df_prematuros_2021, df_prematuros_aux)

# Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(df_prematuros_completo, 'R/databases/Prematuridade_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Tipo de gestação --------------------------------------------------------
df_tipo_gravidez_2021 <- read.csv2("R/databases/Tipo_gravidez_muni2021.csv")

df_tipo_gravidez_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_tipo_gravidez_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gravidez', 'Nascidos')

sort(unique(df_tipo_gravidez_2021$Gravidez), na.last = TRUE)
sort(unique(df_tipo_gravidez_aux$Gravidez), na.last = TRUE)

df_tipo_gravidez_completo <- full_join(df_tipo_gravidez_2021, df_tipo_gravidez_aux)

# Exportando os dados 
write.table(df_tipo_gravidez_completo, 'R/databases/Tipo_gravidez_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Tipo de parto -----------------------------------------------------------
df_tipo_parto_2021 <- read.csv2("R/databases/Tipo_parto_muni2021.csv")

df_tipo_parto_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_tipo_parto_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Parto', 'Nascidos')

sort(unique(df_tipo_parto_2021$Parto), na.last = TRUE)
sort(unique(df_tipo_parto_aux$Parto), na.last = TRUE)

df_tipo_parto_completo <- full_join(df_tipo_parto_2021, df_tipo_parto_aux)

# Exportando os dados 
write.table(df_tipo_parto_completo, 'R/databases/Tipo_parto_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Número de consultas de pré-natal ----------------------------------------
df_consultas_2021 <- read.csv2("R/databases/Consultas_PreNatal_muni2021.csv")

df_consultas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Consultas', 'Nascidos')

sort(unique(df_consultas_2021$Consultas), na.last = TRUE)
sort(unique(df_consultas_aux$Consultas), na.last = TRUE)

df_consultas_completo <- full_join(df_consultas_2021, df_consultas_aux)

# Exportando os dados 
write.table(df_consultas_completo, 'R/databases/Consultas_PreNatal_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Sexo fetal --------------------------------------------------------------
df_sexo_fetal_2021 <- read.csv2("R/databases/Sexo_fetal_muni2021.csv")

df_sexo_fetal_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_sexo_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Sexo', 'Nascidos')

sort(unique(df_sexo_fetal_2021$Sexo), na.last = TRUE)
sort(unique(df_sexo_fetal_aux$Sexo), na.last = TRUE)

df_sexo_fetal_completo <- full_join(df_sexo_fetal_2021, df_sexo_fetal_aux)

# Exportando os dados 
write.table(df_sexo_fetal_completo, 'R/databases/Sexo_fetal_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Apgar 1 -----------------------------------------------------------------
df_apgar1_2021 <- read.csv2("R/databases/Apgar1_muni2021.csv")

df_apgar1_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_apgar1_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar1', 'Nascidos')

sort(unique(df_apgar1_2021$Apgar1), na.last = TRUE)
sort(unique(df_apgar1_aux$Apgar1), na.last = TRUE)

df_apgar1_completo <- full_join(df_apgar1_2021, df_apgar1_aux)

# Exportando os dados 
write.table(df_apgar1_completo, 'R/databases/Apgar1_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Apgar 5 -----------------------------------------------------------------
df_apgar5_2021 <- read.csv2("R/databases/Apgar5_muni2021.csv")

df_apgar5_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_apgar5_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar5', 'Nascidos')

sort(unique(df_apgar5_2021$Apgar5), na.last = TRUE)
sort(unique(df_apgar5_aux$Apgar5), na.last = TRUE)

df_apgar5_completo <- full_join(df_apgar5_2021, df_apgar5_aux)

# Exportando os dados 
write.table(df_apgar5_completo, 'R/databases/Apgar5_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Anomalias ---------------------------------------------------------------
df_anomalias_2021 <- read.csv2("R/databases/Anomalias_muni2021.csv")

df_anomalias_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_anomalias_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Anomalia', 'Nascidos')

sort(unique(df_anomalias_2021$Anomalia), na.last = TRUE)
sort(unique(df_anomalias_aux$Anomalia), na.last = TRUE)

df_anomalias_completo <- full_join(df_anomalias_2021, df_anomalias_aux)

# Exportando os dados 
write.table(df_anomalias_completo, 'R/databases/Anomalias_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Peso fetal menor que 2500g ----------------------------------------------
df_peso_fetal_2021 <- read.csv2("R/databases/Peso_menor_2500_muni2021.csv")

df_peso_fetal_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PESO) |>
  filter(PESO < 2500) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_peso_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')

df_peso_fetal_completo <- full_join(df_peso_fetal_2021, df_peso_fetal_aux)

# Exportando os dados 
write.table(df_peso_fetal_completo, 'R/databases/Peso_menor_2500_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Raça/cor da mãe ---------------------------------------------------------
df_racamae_2021 <- read.csv2("R/databases/Raca_mae_muni2021.csv")

df_racamae_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_racamae_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Raca_mae', 'Nascidos')

sort(unique(df_racamae_2021$Raca_mae), na.last = TRUE)
sort(unique(df_racamae_aux$Raca_mae), na.last = TRUE)

df_racamae_completo <- full_join(df_racamae_2021, df_racamae_aux)

# Exportando os dados 
write.table(df_racamae_completo, 'R/databases/Raca_mae_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Robson ------------------------------------------------------------------
df_robson_2021 <- read.csv2("R/databases/Robson_muni2021.csv")

df_robson_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_robson_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Nascidos')

sort(unique(df_robson_2021$Robson), na.last = TRUE)
sort(unique(df_robson_aux$Robson), na.last = TRUE)

df_robson_completo <- full_join(df_robson_2021, df_robson_aux)

# Exportando os dados 
write.table(df_robson_completo, 'R/databases/Robson_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Parto prematuro ---------------------------------------------------------
df_prematuro_pcdas_2021 <- read.csv2("R/databases/Prematuro_PCDAS_muni2021.csv")

df_prematuro_pcdas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuro_pcdas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')

sort(unique(df_prematuro_pcdas_2021$Gestacao), na.last = TRUE)
sort(unique(df_prematuro_pcdas_aux$Gestacao), na.last = TRUE)

df_prematuro_pcdas_completo <- full_join(df_prematuro_pcdas_2021, df_prematuro_pcdas_aux)

# Exportando os dados 
write.table(df_prematuro_pcdas_completo, 'R/databases/Prematuro_PCDAS_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Cesárea eletiva ---------------------------------------------------------
df_ces_2021 <- read.csv2("R/databases/Cesaria_antes_do_parto_muni2021.csv")

df_ces_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
  filter(TPROBSON %in% c(2, 4)) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_ces_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'cesarea_antes_do_parto', 'Nascidos')

sort(unique(df_ces_2021$cesarea_antes_do_parto), na.last = TRUE)
sort(unique(df_ces_aux$cesarea_antes_do_parto), na.last = TRUE)

df_ces_completo <- full_join(df_ces_2021, df_ces_aux)

# Exportando os dados
write.table(df_ces_completo, 'R/databases/Cesaria_antes_do_parto_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Partos induzidos --------------------------------------------------------
df_parto_induzido_2021 <- read.csv2("R/databases/Parto_induzido_muni2021.csv")

df_parto_induzido_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
  filter(TPROBSON %in% c(2, 4)) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup()

names(df_parto_induzido_aux) <- c('UF','Municipio', 'Codigo', 'Ano', 'Robson', 'parto_induzido', 'Nascidos')

sort(unique(df_parto_induzido_2021$parto_induzido), na.last = TRUE)
sort(unique(df_parto_induzido_aux$parto_induzido), na.last = TRUE)

df_parto_induzido_completo <- full_join(df_parto_induzido_2021, df_parto_induzido_aux)

# Exportando os dados
write.table(df_parto_induzido_completo, 'R/databases/Parto_induzido_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Prematuridade consultas -------------------------------------------------
df_prematuridade_consultas_2021 <- read.csv2("R/databases/Prematuridade_consultas_muni2021.csv")

df_prematuridade_consultas_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_prematuridade_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Consultas', 'Nascidos')

sort(unique(df_prematuridade_consultas_2021$Gestacao), na.last = TRUE)
sort(unique(df_prematuridade_consultas_aux$Gestacao), na.last = TRUE)

sort(unique(df_prematuridade_consultas_2021$Consultas), na.last = TRUE)
sort(unique(df_prematuridade_consultas_aux$Consultas), na.last = TRUE)

df_prematuridade_consultas_completo <- full_join(df_prematuridade_consultas_2021, df_prematuridade_consultas_aux)

# Exportando os dados 
write.table(df_prematuridade_consultas_completo, 'R/databases/Prematuridade_consultas_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)


# Prematuros, cesáreas e Robson -------------------------------------------
df_robson_cesar_2021 <- read.csv2("R/databases/Robson_cesar_muni2021.csv")

df_robson_cesar_aux <- dados_preliminares |>
  select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
  mutate(nascidos = 1) |>
  group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
  summarise(nascidos = sum(nascidos)) |>
  ungroup() 

names(df_robson_cesar_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Parto', 'Nascidos')

df_robson_cesar_completo <- full_join(df_robson_cesar_2021, df_robson_cesar_aux)

# Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(df_robson_cesar_completo, 'R/databases/Robson_cesar_muni2024.csv', sep = ";", dec = ".", row.names = FALSE)

