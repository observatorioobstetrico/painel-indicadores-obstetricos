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

# Baixando os dados consolidados do SINASC --------------------------------
anos <- 1996:2022

for (ano in anos) {
  df_sinasc <- fetch_datasus(
    year_start = ano,
    year_end = ano,
    information_system = "SINASC"
  ) |>
    mutate_if(is.numeric, as.character)
  
  ## Criando as variáveis que não existem e as preenchendo com NA, se for o caso
  df_sinasc[setdiff(variaveis_sinasc, names(df_sinasc))] <- NA
  
  ## Transformando algumas variáveis
  dados_consolidados <- left_join(df_sinasc, df_aux_municipios) |>
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
  df_nascidos_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_nascidos_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')

  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_nascidos_aux, 'R/databases/Nascimentos_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_nascidos_aux,
        read.csv2('R/databases/Nascimentos_muni_consolidados.csv')
      ),
      'R/databases/Nascimentos_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Nascidos vivos prematuros -----------------------------------------------
  df_prematuros_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_prematuros_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')
  
  sort(unique(df_prematuros_aux$Gestacao), na.last = TRUE)
  
  ### Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
  if (ano == 1996) {
    write.table(df_prematuros_aux, 'R/databases/Prematuridade_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_prematuros_aux,
        read.csv2('R/databases/Prematuridade_muni_consolidados.csv')
      ),
      'R/databases/Prematuridade_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }

  
  ## Tipo de gestação --------------------------------------------------------
  df_tipo_gravidez_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GRAVIDEZ) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_tipo_gravidez_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gravidez', 'Nascidos')
  
  sort(unique(df_tipo_gravidez_aux$Gravidez), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_tipo_gravidez_aux, 'R/databases/Tipo_gravidez_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_tipo_gravidez_aux,
        read.csv2('R/databases/Tipo_gravidez_muni_consolidados.csv')
      ),
      'R/databases/Tipo_gravidez_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Tipo de parto -----------------------------------------------------------
  df_tipo_parto_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PARTO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_tipo_parto_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Parto', 'Nascidos')
  
  sort(unique(df_tipo_parto_aux$Parto), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_tipo_parto_aux, 'R/databases/Tipo_parto_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_tipo_parto_aux,
        read.csv2('R/databases/Tipo_parto_muni_consolidados.csv')
      ),
      'R/databases/Tipo_parto_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Número de consultas de pré-natal ----------------------------------------
  df_consultas_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, CONSULTAS) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Consultas', 'Nascidos')

  sort(unique(df_consultas_aux$Consultas), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_consultas_aux, 'R/databases/Consultas_PreNatal_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_consultas_aux,
        read.csv2('R/databases/Consultas_PreNatal_muni_consolidados.csv')
      ),
      'R/databases/Consultas_PreNatal_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Sexo fetal --------------------------------------------------------------
  df_sexo_fetal_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, SEXO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_sexo_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Sexo', 'Nascidos')

  sort(unique(df_sexo_fetal_aux$Sexo), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_sexo_fetal_aux, 'R/databases/Sexo_fetal_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_sexo_fetal_aux,
        read.csv2('R/databases/Sexo_fetal_muni_consolidados.csv')
      ),
      'R/databases/Sexo_fetal_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Apgar 1 -----------------------------------------------------------------
  df_apgar1_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
    mutate(nascidos = 1, APGAR1 = ifelse(APGAR1 == "\xe8", "è", APGAR1)) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR1) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup()
  
  names(df_apgar1_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar1', 'Nascidos')
  
  sort(unique(df_apgar1_aux$Apgar1), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_apgar1_aux, 'R/databases/Apgar1_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_apgar1_aux,
        read.csv2('R/databases/Apgar1_muni_consolidados.csv')
      ),
      'R/databases/Apgar1_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Apgar 5 -----------------------------------------------------------------
  df_apgar5_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
    mutate(nascidos = 1, APGAR5 = ifelse(APGAR5 == "\x81z", "\u0081z", APGAR5)) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, APGAR5) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup()
  
  names(df_apgar5_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar5', 'Nascidos')

  sort(unique(df_apgar5_aux$Apgar5), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_apgar5_aux, 'R/databases/Apgar5_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_apgar5_aux,
        read.csv2('R/databases/Apgar5_muni_consolidados.csv')
      ),
      'R/databases/Apgar5_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Anomalias ---------------------------------------------------------------
  df_anomalias_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, IDANOMAL) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_anomalias_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Anomalia', 'Nascidos')

  sort(unique(df_anomalias_aux$Anomalia), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_anomalias_aux, 'R/databases/Anomalias_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_anomalias_aux,
        read.csv2('R/databases/Anomalias_muni_consolidados.csv')
      ),
      'R/databases/Anomalias_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Peso fetal menor que 2500g ----------------------------------------------
  df_peso_fetal_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, PESO) |>
    filter(PESO < 2500) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_peso_fetal_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_peso_fetal_aux, 'R/databases/Peso_menor_2500_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_peso_fetal_aux,
        read.csv2('R/databases/Peso_menor_2500_muni_consolidados.csv')
      ),
      'R/databases/Peso_menor_2500_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Raça/cor da mãe ---------------------------------------------------------
  df_racamae_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, RACACORMAE) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_racamae_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Raca_mae', 'Nascidos')

  sort(unique(df_racamae_aux$Raca_mae), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_racamae_aux, 'R/databases/Raca_mae_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_racamae_aux,
        read.csv2('R/databases/Raca_mae_muni_consolidados.csv')
      ),
      'R/databases/Raca_mae_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Robson ------------------------------------------------------------------
  df_robson_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_robson_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Nascidos')
  
  sort(unique(df_robson_aux$Robson), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_robson_aux, 'R/databases/Robson_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_robson_aux,
        read.csv2('R/databases/Robson_muni_consolidados.csv')
      ),
      'R/databases/Robson_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Parto prematuro ---------------------------------------------------------
  df_prematuro_pcdas_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_prematuro_pcdas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')

  sort(unique(df_prematuro_pcdas_aux$Gestacao), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_prematuro_pcdas_aux, 'R/databases/Prematuro_PCDAS_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_prematuro_pcdas_aux,
        read.csv2('R/databases/Prematuro_PCDAS_muni_consolidados.csv')
      ),
      'R/databases/Prematuro_PCDAS_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Cesárea eletiva ---------------------------------------------------------
  df_ces_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
    filter(TPROBSON %in% c(2, 4)) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STCESPARTO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_ces_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'cesarea_antes_do_parto', 'Nascidos')
  
  sort(unique(df_ces_aux$cesarea_antes_do_parto), na.last = TRUE)
  
  ### Exportando os dados
  if (ano == 1996) {
    write.table(df_ces_aux, 'R/databases/Cesaria_antes_do_parto_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_ces_aux,
        read.csv2('R/databases/Cesaria_antes_do_parto_muni_consolidados.csv', colClasses = c(rep("character", 2), rep("double", 5)))
      ),
      'R/databases/Cesaria_antes_do_parto_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Partos induzidos --------------------------------------------------------
  df_parto_induzido_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
    filter(TPROBSON %in% c(2, 4)) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, STTRABPART) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup()
  
  names(df_parto_induzido_aux) <- c('UF','Municipio', 'Codigo', 'Ano', 'Robson', 'parto_induzido', 'Nascidos')

  sort(unique(df_parto_induzido_aux$parto_induzido), na.last = TRUE)
  
  ### Exportando os dados
  if (ano == 1996) {
    write.table(df_parto_induzido_aux, 'R/databases/Parto_induzido_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_parto_induzido_aux,
        read.csv2('R/databases/Parto_induzido_muni_consolidados.csv', colClasses = c(rep("character", 2), rep("double", 5)))
      ),
      'R/databases/Parto_induzido_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Prematuridade consultas -------------------------------------------------
  df_prematuridade_consultas_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, GESTACAO, CONSULTAS) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_prematuridade_consultas_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Consultas', 'Nascidos')
  
  sort(unique(df_prematuridade_consultas_aux$Gestacao), na.last = TRUE)
  sort(unique(df_prematuridade_consultas_aux$Consultas), na.last = TRUE)
  
  ### Exportando os dados 
  if (ano == 1996) {
    write.table(df_prematuridade_consultas_aux, 'R/databases/Prematuridade_consultas_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_prematuridade_consultas_aux,
        read.csv2('R/databases/Prematuridade_consultas_muni_consolidados.csv')
      ),
      'R/databases/Prematuridade_consultas_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  
  ## Prematuros, cesáreas e Robson -------------------------------------------
  df_robson_cesar_aux <- dados_consolidados |>
    select(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
    mutate(nascidos = 1) |>
    group_by(res_SIGLA_UF, res_MUNNOMEX, CODMUNRES, ano_nasc, TPROBSON, PARTO) |>
    summarise(nascidos = sum(nascidos)) |>
    ungroup() 
  
  names(df_robson_cesar_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Parto', 'Nascidos')
  
  ### Exportando os dados
  if (ano == 1996) {
    write.table(df_robson_cesar_aux, 'R/databases/Robson_cesar_muni_consolidados.csv', sep = ";", dec = ".", row.names = FALSE)
  } else {
    write.table(
      bind_rows(
        df_robson_cesar_aux,
        read.csv2('R/databases/Robson_cesar_muni_consolidados.csv')
      ),
      'R/databases/Robson_cesar_muni_consolidados.csv', 
      sep = ";", dec = ".", row.names = FALSE
    )
  }
  
  ## Limpando a memória
  rm(list = setdiff(ls(pattern = "^df|dados_consolidados"), "df_aux_municipios"))
  gc()
}


