library(dplyr)
library(sqldf)
library(httr)
library(ggplot2)
library(getPass)
library(repr)
library(data.table)

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

#numero de nascimentos
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  # Nascimentos total por município
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc", "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_mun <- convertRequestToDF(request)
  names(df_mun) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')
  dataframe <- rbind(dataframe, df_mun)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_mun <- convertRequestToDF(request)
    names(df_mun) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')
    dataframe <- rbind(dataframe, df_mun)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(dataframe, 'R/databases/Nascimentos_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##prematuros
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_premat <- convertRequestToDF(request)
  names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')
  dataframe <- rbind(dataframe, df_premat)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_premat <- convertRequestToDF(request)
    names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')
    dataframe <- rbind(dataframe, df_premat)
  }
}

head(dataframe)

# Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(dataframe, 'R/databases/Prematuridade_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##tipo de gestacao
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GRAVIDEZ, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GRAVIDEZ", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gravidez', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GRAVIDEZ, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GRAVIDEZ", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gravidez', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

# Exportando os dados 
write.table(dataframe, 'R/databases/Tipo_gravidez_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##tipo de parto
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, PARTO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, PARTO", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Parto', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, PARTO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, PARTO", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Parto', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Tipo_parto_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##numero de consultas
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, CONSULTAS, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, CONSULTAS", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Consultas', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, CONSULTAS, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, CONSULTAS", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Consultas', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Consultas_PreNatal_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##sexo fetal
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, SEXO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, SEXO", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Sexo', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, SEXO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, SEXO", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Sexo', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Sexo_fetal_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##Apgar1
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR1, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR1", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar1', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR1, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR1", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar1', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Apgar1_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##Apgar5
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR5, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR5", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar5', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR5, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, APGAR5", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Apgar5', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Apgar5_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##Anomalia
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, IDANOMAL, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, IDANOMAL", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Anomalia', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, IDANOMAL, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, IDANOMAL", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Anomalia', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Anomalias_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##peso fetal menor 2500
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' AND (PESO < 2500) GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano','Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc,  COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' AND (PESO < 2500) GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Peso_menor_2500_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##RACACORMAE
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, RACACORMAE, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, RACACORMAE", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Raca_mae', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, RACACORMAE, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, RACACORMAE", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Raca_mae', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Raca_mae_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##Robson
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Robson_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##parto_prematuro
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_aux <- convertRequestToDF(request)
  names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')
  dataframe <- rbind(dataframe, df_aux)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_aux <- convertRequestToDF(request)
    names(df_aux) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Nascidos')
    dataframe <- rbind(dataframe, df_aux)
  }
}

head(dataframe)
dim(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Prematuro_PCDAS_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)


## partos induzidos por município 
## cesárea eletiva (antes do trabalho de parto): 1 (sim)
params = paste0('{
        "token": {
          "token": "',token,'"
        },
        "sql": {
          "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, STCESPARTO, COUNT(1)',
                ' FROM \\"datasus-sinasc\\"', 
                ' WHERE TPROBSON in (2, 4)',
                ' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, STCESPARTO" }
        }
      }')
endpoint <- paste0(url_base,"/","sql_query")
request <- POST(url = endpoint,
                body = params,
                encode = "form")
df_ces1 <- convertRequestToDF(request)
names(df_ces1) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'cesarea_antes_do_parto', 'Nascidos')
head(df_ces1)

# Exportando os dados
write.table(df_ces1, 'R/databases/Cesaria_antes_do_parto_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

## partos induzidos por município 
params = paste0('{
        "token": {
          "token": "',token,'"
        },
        "sql": {
          "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, STTRABPART, COUNT(1)',
                ' FROM \\"datasus-sinasc\\"',
                ' WHERE TPROBSON in (2, 4)',
                ' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, STTRABPART" }
        }
      }')
endpoint <- paste0(url_base,"/","sql_query")
request <- POST(url = endpoint,
                body = params,
                encode = "form")
df_trab1 <- convertRequestToDF(request)
names(df_trab1) <- c('UF','Municipio', 'Codigo', 'Ano', 'Robson', 'parto_induzido', 'Nascidos')
head(df_trab1)

# Exportando os dados
write.table(df_trab1, 'R/databases/Parto_induzido_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

##prematuridade consultas
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, CONSULTAS, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, CONSULTAS", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_premat <- convertRequestToDF(request)
  names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Consultas', 'Nascidos')
  dataframe <- rbind(dataframe, df_premat)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, CONSULTAS, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, GESTACAO, CONSULTAS", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_premat <- convertRequestToDF(request)
    names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Gestacao', 'Consultas', 'Nascidos')
    dataframe <- rbind(dataframe, df_premat)
  }
}

head(dataframe)

# Exportando os dados 
write.table(dataframe, 'R/databases/Prematuridade_consultas_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

## prematuros, cesareas e robson
estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
dataframe <- dataframe2 <- data.frame()

endpoint <- paste0(url_base,"/","sql_query")

for (estado in estados){
  params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, PARTO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, PARTO", "fetch_size": 65000}
          }
          
        }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_premat <- convertRequestToDF(request)
  names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Parto', 'Nascidos')
  dataframe <- rbind(dataframe, df_premat)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, PARTO, COUNT(1) FROM \\"datasus-sinasc\\" WHERE res_SIGLA_UF = \'',estado,'\' GROUP BY res_SIGLA_UF, res_MUNNOMEX, res_codigo_adotado, ano_nasc, TPROBSON, PARTO", "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_premat <- convertRequestToDF(request)
    names(df_premat) <- c('UF', 'Municipio', 'Codigo', 'Ano', 'Robson', 'Parto', 'Nascidos')
    dataframe <- rbind(dataframe, df_premat)
  }
}

head(dataframe)

# Exportando os dados (Nascimentos_muni = UF, Município, Ano e Nascimentos)
write.table(dataframe, 'R/databases/Robson_cesar_muni2021.csv', sep = ";", dec = ".", row.names = FALSE)

