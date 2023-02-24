library(tidyverse)
library(writexl)
library(readxl)
library(lubridate)
library(crunch)
library(janitor)

# Criando função soma_var para poder fazer tratamento dos dados ----
soma_var <- function(vars,dados) {
  out <- rowSums(dados[,vars],na.rm=TRUE)
  return(out)
}

# Tratamento dos dados até o ano de 2022 ----

## Nascimentos ----
(dados_nasc <- read_delim("./databases/Nascimentos_muni2022.csv", ";", 
                          escape_double = FALSE, trim_ws = TRUE))

write_xlsx(dados_nasc, "./databases/Nascimentos_muni_wide.xlsx")

## Prematuridade ----
(dados <- read_delim("./databases/Prematuridade_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Gestacao, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      premat = soma_var(c("1","2","3","4"), dados1), 
      nao_premat = soma_var(c("5","6"), dados1),
      faltante_premat = soma_var(c("9"), dados1),
      total_nascidos = soma_var(c("1","2","3","4","5","6","9"), dados1),
      porc_premat = (premat/(total_nascidos-faltante_premat))*100) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/prematuridade_muni_wide.xlsx")

## Tipo de parto ----
(dados <- read_delim("./databases/Tipo_parto_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Parto, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      cesarea = soma_var(c("2"),dados1), 
      vaginal = soma_var(c("1"),dados1),
      faltante_tipo_parto = soma_var(c("9"),dados1),
      total_nascidos = soma_var(c("1","2","9"),dados1),
      porc_cesarea = (cesarea/(total_nascidos-faltante_tipo_parto))*100) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/cesarea_muni_wide.xlsx")

## Apgar1 ----
(dados <- read_delim("./databases/Apgar1_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Apgar1, Nascidos))

(dados2 <- dados1 %>%
    mutate(
      apgar1_menor_7 = soma_var(c("00", "01", "02", "03", "04", "05", "06", "0", "1", "2", "4", "5", "6"),dados1),
      apgar1_maior_7 = soma_var(c("07", "08", "09", "7", "8", "9", "10"), dados1),
      faltantes_apgar1 = soma_var(c("99", "<NA>", "-", "--", "-0", ".", "..", "+", "0Q", "30", "52", "61", "89", "\0209", "è"), dados1),
      porc_apgar1_menor_7 = (apgar1_menor_7 / (apgar1_menor_7 + apgar1_maior_7))*100) %>%
    select(-"<NA>", -"0Q") %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/apgar1_muni_wide.xlsx")

## Apgar5 ----
(dados <- read_delim("./databases/Apgar5_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Apgar5, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      apgar5_menor_7 = soma_var(c("00","01","02","03","04","05","06", "0", "2", "3", "4", "5", "6"),dados1), 
      apgar5_maior_7 = soma_var(c("07","08","09","10", "7", "8", "9"),dados1),
      faltantes_apgar5 = soma_var(c("99", "<NA>", "\027", "-", "--", ".", "..", "+", "0-", "0+", "11", "\u0081z", "0R"), dados1),
      porc_apgar5_menor_7 = (apgar5_menor_7/(apgar5_menor_7+apgar5_maior_7))*100) %>% 
    select(-"<NA>", -"\u0081z", -"0R") %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")]) 

write_xlsx(dados2, "./databases/apgar5_muni_wide.xlsx")

## Anomalias ----
(dados <- read_delim("./databases/Anomalias_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Anomalia, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      anomalia = soma_var(c("1"),dados1), 
      nao_anomalia = soma_var(c("2"),dados1),
      faltante_anomalia = soma_var(c("9", "<NA>"),dados1),
      total_nascidos = soma_var(c("1", "2", "9", "<NA>"),dados1),
      porc_anomalia = (anomalia/(total_nascidos-faltante_anomalia))*100) %>%
    select(-"<NA>") %>%   
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

### Correção: Anomalias só existe na base a partir de 2001 ----
(dados2 <- dados2 %>% 
   mutate(porc_anomalia = ifelse(Ano < 2001, NA, porc_anomalia)))

write_xlsx(dados2, "./databases/anomalia_muni_wide.xlsx")

## Peso menor 2500 ----
(dados <- read_delim("./databases/Peso_menor_2500_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados <- dados %>% 
    rename(peso_menor_2500 = Nascidos))

(dados1 <- full_join(dados, dados_nasc, by = c("UF","Municipio", "Codigo", "Ano")))

(dados2 <- dados1 %>% 
    mutate(
      porc_peso_menor_2500 = (peso_menor_2500/(Nascidos))*100
    ) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/peso_menor_2500_muni_wide.xlsx")

## Sexo ----
(dados <- read_delim("./databases/Sexo_fetal_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Sexo, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      fem = soma_var(c("2"),dados1), 
      masc = soma_var(c("1"),dados1),
      faltante_sexo = soma_var(c("0"),dados1),
      total_nascidos = soma_var(c("1", "2", "0"),dados1),
      porc_fem = (fem/(total_nascidos-faltante_sexo))*100) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/sexo_muni_wide.xlsx")

## Consultas ----
(dados <- read_delim("./databases/Consultas_PreNatal_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Consultas, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      nenhuma_consulta = soma_var(c("1"), dados1), # nenhuma consulta
      consulta1 = soma_var(c("2", "3"), dados1), # 1 a 6 consultas
      consulta4 = soma_var(c("4"), dados1), # 7 ou mais 
      faltante_consulta = soma_var(c("9"), dados1),
      porc_nenhuma_consulta = nenhuma_consulta/(soma_var(c("1", "2", "3", "4"), dados1)),
      porc_consulta1 = consulta1/(soma_var(c("1", "2", "3", "4"), dados1)),
      porc_consulta4 = consulta4/(soma_var(c("1", "2", "3", "4"), dados1))
    ) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/consultas_muni_wide.xlsx")

## Raça da mae ----

#---------------------------------------#

# Raça da mae

# Raça/Cor: 1:Branca; 2:Preta; 3:Amarela; 4: Parda; 5: Indígena

#---------------------------------------#

(dados <- read_delim("./databases/Raca_mae_muni2022.csv",
                     ";",
                     escape_double = FALSE,
                     trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Raca_mae, Nascidos))

(dados2 <- dados1 %>%
    mutate(
      raca_mae_branca = soma_var(c("1"), dados1),
      raca_mae_preta = soma_var(c("2"), dados1),
      raca_mae_amarela = soma_var(c("3"), dados1),
      raca_mae_parda = soma_var(c("4"), dados1),
      raca_mae_indigena = soma_var(c("5"), dados1),
      faltante_raca_mae = soma_var(c("9", "<NA>"), dados1),
      total_nascidos = soma_var(c("1", "2", "3", "4", "5", "9", "<NA>"), dados1),
      porc_raca_mae_branca = (raca_mae_branca / (total_nascidos - faltante_raca_mae)) *
        100,
      porc_raca_mae_negra = ((raca_mae_preta + raca_mae_parda) / (total_nascidos -
                                                                    faltante_raca_mae)) * 100
    ) %>%
    select(-"<NA>") %>%
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

dados2$porc_raca_mae_branca[is.nan(dados2$porc_raca_mae_branca)] <-
  NA
dados2$porc_raca_mae_negra[is.nan(dados2$porc_raca_mae_negra)] <-
  NA

write_xlsx(dados2, "./databases/raca_mae_muni_wide.xlsx")

## Tipo de gravidez ----

#---------------------------------------#
# Tipo de gravidez (unica ou multipla)
# Tipo de gravidez, conforme a tabela:9: Ignorado 1: Única2: Dupla3: Tripla e mais
#---------------------------------------#

(dados <- read_delim("./databases/Tipo_gravidez_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Gravidez, Nascidos))

(dados2 <- dados1 %>% 
    mutate(
      unica = soma_var(c("1"),dados1), 
      multipla = soma_var(c("2", "3"),dados1),
      faltante_tipo_gravidez = soma_var(c("9"),dados1),
      total_nascidos = soma_var(c("1","2", "3","9"),dados1),
      porc_multipla = (multipla/(total_nascidos-faltante_tipo_gravidez))*100) %>% 
    .[, str_detect(names(.), pattern = "[A-Z]|[a-z]")])

write_xlsx(dados2, "./databases/tipo_gravidez_muni_wide.xlsx")

## Robson ----

(dados <- read_delim("./databases/Robson_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados_induzido <- read_delim("./databases/Parto_induzido_muni2022.csv", ";",
                              escape_double = FALSE, trim_ws = TRUE) %>% 
    rename(nascidos_parto_induzido = Nascidos))

(dados <- left_join(dados, dados_induzido, by = c("UF", "Municipio", "Codigo", "Ano", "Robson")))

(dados1 <- dados %>%
    pivot_wider(names_from = Robson, values_from = Nascidos, values_fill = 0,
                names_prefix = "robson_"))

(dados2 <- dados1 %>% 
    mutate(
      faltante_robson = soma_var(c("robson_11", "robson_12", "robson_NA"), dados1)) %>% 
    select(-"robson_NA", -"robson_11", -"robson_12")
)

### Robson 2 e 4 com parto induzido ----

# A variável STTRABPART diz se houve trabalho de parto induzido com as seguintes respostas possíveis:
# Valores: 1- Sim; 2- Não; 3- Não se aplica; 9- Ignorado.

(dados1 <- dados %>% 
   mutate(parto_induzido = case_when(parto_induzido %in% c(3, 4) ~ "faltante",
                                     parto_induzido == 1 ~ "sim",
                                     parto_induzido == 2 ~ "não")) %>% 
   group_by(UF, Municipio, Codigo, Ano, Robson, parto_induzido) %>% 
   summarise(Nascidos = sum(Nascidos)) %>% 
   rename(tipo_robson = Robson, nascidos_parto_induzido = Nascidos) %>% 
   janitor::clean_names()) 

write_xlsx(dados2, "./databases/robson_muni_wide.xlsx")

### Concatenando todos os dados
dados_sinasc2021 <- read_excel("./databases/Nascimentos_muni_wide.xlsx")

arquivos <- dir("./databases") %>% 
  str_subset("\\.xlsx$") %>% 
  .[!str_detect(., pattern = "Nascimentos_muni_wide.xlsx")]

for(arquivo in arquivos){
  dados <- read_excel(paste0("./databases/", arquivo))
  dados_sinasc2021 <- full_join(dados_sinasc2021, dados, by = c("UF", "Municipio", "Codigo", "Ano"))
  print(paste("Junção", arquivo))
}

names(dados_sinasc2021)

if("Nascidos.x" %in% names(dados_sinasc2021)){
  if(sum(dados_sinasc2021[["Nascidos.x"]], -dados_sinasc2021[["Nascidos.y"]]) == 0){
    if("Nascidos" %in% names(dados_sinasc2021)){
      dados_sinasc2021 <- dados_sinasc2021 %>% 
        .[, !str_detect(names(.), "Nascidos.")]
    }
    else{
      dados_sinasc2021 <- dados_sinasc2021 %>% 
        rename(Nascidos = Nascidos.x) %>%  
        .[, !str_detect(names(.), "Nascidos.")]
    }
  }
  else{
    print("Diferença não é igual a zero")
  }
}


if("total_nascidos.x" %in% names(dados_sinasc2021)){
  if(sum(dados_sinasc2021[["total_nascidos.x"]], -dados_sinasc2021[["total_nascidos.y"]]) == 0){
    if("total_nascidos" %in% names(dados_sinasc2021)){
      dados_sinasc2021 <- dados_sinasc2021 %>% 
        .[, !str_detect(names(.), "total_nascidos.")]
    }
    else{
      dados_sinasc2021 <- dados_sinasc2021 %>% 
        rename(total_nascidos = total_nascidos.x) %>%  
        .[, !str_detect(names(.), "total_nascidos.")]
    }
  }
  else{
    print("Diferença não é igual a zero")
  }
}

dados_sinasc2021 <- dados_sinasc2021 %>%
  mutate(pais = "Brasil") %>%
  mutate(muni_estado = paste(Municipio, "-", UF)) %>% 
  janitor::clean_names()

names(dados_sinasc2021)              

janitor::get_dupes(dados_sinasc2021, c(uf, municipio, codigo, ano))

dados_sinasc2021 <- distinct(dados_sinasc2021, uf, municipio, codigo, ano, .keep_all = TRUE)

janitor::get_dupes(dados_sinasc2021, c(uf, municipio, codigo, ano))

crunch::write.csv.gz(dados_sinasc2021, file = "dados_sinasc.csv.gz")

# Tratamento base consultas e prematuridade ----

## Consultas ----

(dados <- read_delim("./databases/Consultas_PreNatal_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    spread(Consultas, Nascidos))

(dados_cons <- dados1 %>% 
    mutate(
      nenhuma_consulta = soma_var(c("1"), dados1), # nenhuma consulta
      consulta1 = soma_var(c("2", "3"), dados1), # 1 a 6 consultas
      consulta4 = soma_var(c("4"), dados1), # 7 ou mais 
      faltante_consulta = soma_var(c("9"), dados1),
      porc_nenhuma_consulta = nenhuma_consulta/(soma_var(c("1", "2", "3", "4"), dados1)),
      porc_consulta1 = consulta1/(soma_var(c("1", "2", "3", "4"), dados1)),
      porc_consulta4 = consulta4/(soma_var(c("1", "2", "3", "4"), dados1))
    ) %>% 
    select(UF, Municipio, Codigo, Ano, faltante_consulta, nenhuma_consulta, consulta1, consulta4))

## Prematuridade consultas ----

(dados <- read_delim("./databases/Prematuridade_consultas_muni2022.csv", ";", 
                     escape_double = FALSE, trim_ws = TRUE))

(dados1 <- dados %>%
    filter(Gestacao <= 4) %>% 
    mutate(n_consultas = case_when(Consultas %in% c("1") ~ "nenhuma",
                                   Consultas %in% c("2", "3") ~ "1_6",
                                   Consultas %in% c("4") ~ "7_mais",
                                   TRUE ~ "faltante"),
           Gestacao = "prematuro") %>% 
    group_by(UF, Municipio, Codigo, Ano, Gestacao, n_consultas) %>% 
    summarise(Nascidos = sum(Nascidos)) %>%
    ungroup() %>% 
    pivot_wider(names_from = c(Gestacao, n_consultas), 
                values_from = Nascidos, 
                values_fill = 0) %>% 
    group_by(UF, Municipio, Codigo, Ano) %>% 
    summarise(prematuro_faltante = sum(prematuro_faltante),
              prematuro_nenhuma = sum(prematuro_nenhuma),
              prematuro_7_mais = sum(prematuro_7_mais),
              prematuro_1_6 = sum(prematuro_1_6)) %>% 
    ungroup()) 

(dados2 <- full_join(dados1, dados_cons, by = c("UF", "Municipio", "Codigo", "Ano")) %>% 
    mutate(porc_consulta1_premat = prematuro_1_6/consulta1*100,
           porc_consulta4_premat = prematuro_7_mais/consulta4*100,
           porc_consulta0_premat = prematuro_nenhuma/nenhuma_consulta*100,
           porc_consulta_faltante_premat = prematuro_faltante/faltante_consulta*100)) 

(dados_final <- full_join(dados2, dados_nasc, by = c("UF", "Municipio", "Codigo", "Ano")))

dados_final <- dados_final %>%
  mutate(pais = "Brasil") %>%
  mutate(muni_estado = paste(Municipio, "-", UF)) %>% 
  janitor::clean_names()

janitor::get_dupes(dados_final, c(uf, municipio, codigo, ano))

crunch::write.csv.gz(dados_final, "prematuridade_consultas_muni.csv.gz")

## Outros arquivos que serão tratadas para serem utilizadas no app de maneira mais leve ----

dados_robson <- read_delim("./databases/Robson_muni2022.csv", ";", 
                           escape_double = FALSE, trim_ws = TRUE)

dados_robson <- dados_robson %>%
  mutate(
    grupo_robson = case_when(
      is.na(Robson) | Robson %in% c("11", "12") ~ NA_real_,
      TRUE ~ as.double(Robson)
    ),
    grupo_robson_aux = case_when(
      is.na(Robson) | Robson %in% c("11", "12") ~ "faltante",
      TRUE ~ as.character(Robson)
    )
  ) %>%
  clean_names() %>%
  group_by(uf, municipio, ano, grupo_robson_aux, grupo_robson) %>%
  summarise(nascidos = sum(nascidos)) %>%
  ungroup()

dados_robson <- dados_robson %>%
  mutate(muni_estado = paste(municipio, "-", uf))

crunch::write.csv.gz(dados_robson, file = "robson_muni.csv.gz")

dados_robson_cesarea <- read_delim("./databases/Robson_cesar_muni2022.csv", ";", 
                                   escape_double = FALSE, trim_ws = TRUE) %>% 
  clean_names()

dados_robson_cesarea <- dados_robson_cesarea %>%
  mutate(
    grupo_robson = case_when(
      is.na(robson) | robson %in% c("11", "12") ~ NA_real_,
      TRUE ~ as.double(robson)
    ),
    grupo_robson_aux = case_when(
      is.na(robson) | robson %in% c("11", "12") ~ "faltante",
      TRUE ~ as.character(robson)
    ),
    tipo_parto = case_when(parto == "2" ~ "cesarea",
                           parto == "1" ~ "vaginal",
                           TRUE ~ "faltante")
  ) %>%
  group_by(uf, municipio, ano, tipo_parto, grupo_robson_aux, grupo_robson) %>%
  summarise(nascidos = sum(nascidos)) %>%
  ungroup()

dados_robson_cesarea <- dados_robson_cesarea %>%
  mutate(pais = "Brasil") %>%
  mutate(muni_estado = paste(municipio, "-", uf))

crunch::write.csv.gz(dados_robson_cesarea, file = "robson_cesar_muni.csv.gz")

# ordenar em ordem alfabetica municipios e estados

municipiosChoices <-
  unique(dados_sinasc2021$muni_estado)[order(unique(dados_sinasc2021$muni_estado))]

saveRDS(municipiosChoices, "municipiosChoices.rds")

estadosChoices <-
  unique(dados_sinasc2021$uf)[order(unique(dados_sinasc2021$uf))]

saveRDS(estadosChoices, "estadosChoices.rds")
