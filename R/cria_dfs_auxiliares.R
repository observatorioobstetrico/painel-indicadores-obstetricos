library(dplyr)
library(tidyverse)
library(getPass)
library(httr)

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

# Obtendo um dataframe com as regiões, UFs e nomes de cada município -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CODMUNRES, res_codigo_adotado, res_MUNNOME, res_MUNNOMEX, res_SIGLA_UF, res_NOME_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CODMUNRES, res_codigo_adotado, res_MUNNOME, res_MUNNOMEX, res_SIGLA_UF, res_NOME_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("CODMUNRES", "res_codigo_adotado", "res_MUNNOME", "res_MUNNOMEX", "res_SIGLA_UF", "res_NOME_UF", "res_REGIAO", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(CODMUNRES)

## Exportando o arquivo
write.csv(df_aux_municipios, "R/databases/df_aux_municipios.csv", row.names = FALSE)
