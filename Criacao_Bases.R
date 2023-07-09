# Bibliotecas ------------------------------------------------------------------
library(tidyverse)
library(sparklyr)

# Diretorio --------------------------------------------------------------------
setwd("C:\\Users\\joao9\\OneDrive\\Documentos\\GitHub\\TCC_Dados_Massivos\\")

# Conexão
sc <- spark_connect(master = "local")


# Importando os dados

sinasc_19 <- spark_read_csv(sc, "nasc19", 
                            "Base_de_Dados\\SINASC_2019.csv",
                            delim = ";", memory = FALSE)

sinasc_18 <- spark_read_csv(sc, "nasc18", 
                            "Base_de_Dados\\SINASC_2018.csv",
                            delim = ";", memory = FALSE)

sinasc_17 <- spark_read_csv(sc, "nasc17", 
                            "Base_de_Dados\\SINASC_2017.csv",
                            delim = ";", memory = FALSE)

sinasc_16 <- spark_read_csv(sc, "nasc16", 
                            "Base_de_Dados\\SINASC_2016.csv",
                            delim = ";", memory = FALSE)

sinasc_15 <- spark_read_csv(sc, "nasc15", 
                            "Base_de_Dados\\SINASC_2015.csv",
                            delim = ";", memory = FALSE)

# Formatando as variáveis

sinasc_19_bs <- sinasc_19 %>%
  mutate(
    ano_ref = 2019,
    
    
    estcv = ESTCIVMAE,   
    idadmae = as.integer(IDADEMAE),             
    idadpai = as.integer(IDADEPAI),             
    codmunre = as.integer(CODMUNRES),           
    
    
    qtgestant = as.integer(QTDGESTANT),         
    qtpartnor = as.integer(QTDPARTNOR),
    qtpartces = as.integer(QTDPARTCES),      
    qtfilvivo = as.integer(QTDFILVIVO),        
    qtfilmot = as.integer(QTDFILMORT),          
    
    
    tpparto = PARTO,  
    conspre = CONSULTAS,  
    
    tpgravidez = GRAVIDEZ,    
    
    mesprent = as.integer(MESPRENAT),          
    
    codmunasc = as.integer(CODMUNNASC),
    sexo = SEXO,        
    
    idanomal_Sim = ifelse(IDANOMAL == 1, 1,0),  
    idanomal_Nao = ifelse(IDANOMAL == 2, 1,0),  
    idanomal_Ign = ifelse(IDANOMAL == 9, 1,0)  
    
  ) %>%
  select(ano_ref, estcv, idadmae , idadpai, codmunre,
         qtgestant, qtpartnor, qtpartces, qtfilvivo, qtfilmot, 
         codmunasc, tpparto, conspre, 
         tpgravidez, mesprent, sexo,
         idanomal_Sim, idanomal_Nao, idanomal_Ign)

sinasc_18_bs <- sinasc_18 %>%
  mutate(
    ano_ref = 2018,
    
    estcv = ESTCIVMAE,    
    
    idadmae = as.integer(IDADEMAE),       
    idadpai = as.integer(IDADEPAI),         
    codmunre = as.integer(CODMUNRES),        
    
    qtgestant = as.integer(QTDGESTANT),       
    qtpartnor = as.integer(QTDPARTNOR),       
    qtpartces = as.integer(QTDPARTCES),   
    qtfilvivo = as.integer(QTDFILVIVO),      
    qtfilmot = as.integer(QTDFILMORT),        
    
    codmunasc = as.integer(CODMUNNASC),      
    tpparto = PARTO,                           
    conspre = CONSULTAS,
    
    tpgravidez = GRAVIDEZ,  
    
    mesprent = as.integer(MESPRENAT),          
    
    sexo = SEXO,                                
    
    idanomal_Sim = ifelse(IDANOMAL == 1, 1,0), 
    idanomal_Nao = ifelse(IDANOMAL == 2, 1,0), 
    idanomal_Ign = ifelse(IDANOMAL == 9, 1,0)  
    
  ) %>%
  select(ano_ref, estcv, idadmae , idadpai, codmunre,
         qtgestant, qtpartnor, qtpartces, qtfilvivo, qtfilmot, 
         codmunasc, tpparto, conspre, 
         tpgravidez, mesprent, sexo,
         idanomal_Sim, idanomal_Nao, idanomal_Ign)

sinasc_17_bs <- sinasc_17 %>%
  mutate(
    ano_ref = 2017,
    
    estcv = ESTCIVMAE,    
    
    idadmae = as.integer(IDADEMAE),      
    idadpai = as.integer(IDADEPAI),        
    codmunre = as.integer(CODMUNRES),       
    
    qtgestant = as.integer(QTDGESTANT),       
    qtpartnor = as.integer(QTDPARTNOR),         
    qtpartces = as.integer(QTDPARTCES),            
    qtfilvivo = as.integer(QTDFILVIVO),            
    qtfilmot = as.integer(QTDFILMORT),          
    
    codmunasc = as.integer(CODMUNNASC),       
    tpparto = PARTO,     
    conspre = CONSULTAS, 
    
    tpgravidez = GRAVIDEZ,   
    
    mesprent = as.integer(MESPRENAT),         
    
    sexo = SEXO,        
    
    idanomal_Sim = ifelse(IDANOMAL == 1, 1,0), 
    idanomal_Nao = ifelse(IDANOMAL == 2, 1,0),  
    idanomal_Ign = ifelse(IDANOMAL == 9, 1,0) 
    
  ) %>%
  select(ano_ref, estcv, idadmae , idadpai, codmunre,
         qtgestant, qtpartnor, qtpartces, qtfilvivo, qtfilmot, 
         codmunasc, tpparto, conspre, 
         tpgravidez, mesprent, sexo,
         idanomal_Sim, idanomal_Nao, idanomal_Ign)

sinasc_16_bs <- sinasc_16 %>%
  mutate(
    ano_ref = 2016,
    
    estcv = ESTCIVMAE,   
    
    idadmae = as.integer(IDADEMAE),             
    idadpai = as.integer(IDADEPAI),            
    codmunre = as.integer(CODMUNRES),         
    
    qtgestant = as.integer(QTDGESTANT),       
    qtpartnor = as.integer(QTDPARTNOR),       
    qtpartces = as.integer(QTDPARTCES),         
    qtfilvivo = as.integer(QTDFILVIVO),         
    qtfilmot = as.integer(QTDFILMORT),         
    
    codmunasc = as.integer(CODMUNNASC),       
    tpparto = PARTO,        
    conspre = CONSULTAS,  
    
    tpgravidez = GRAVIDEZ,
    
    mesprent = as.integer(MESPRENAT),        
    
    sexo = SEXO,           
    
    idanomal_Sim = ifelse(IDANOMAL == 1, 1,0),  
    idanomal_Nao = ifelse(IDANOMAL == 2, 1,0),  
    idanomal_Ign = ifelse(IDANOMAL == 9, 1,0)  
    
  ) %>%
  select(ano_ref, estcv, idadmae , idadpai, codmunre,
         qtgestant, qtpartnor, qtpartces, qtfilvivo, qtfilmot, 
         codmunasc, tpparto, conspre, 
         tpgravidez, mesprent, sexo,
         idanomal_Sim, idanomal_Nao, idanomal_Ign)

sinasc_15_bs <- sinasc_15 %>%
  mutate(
    ano_ref = 2015,
    
    
    estcv = ESTCIVMAE,  
    
    idadmae = as.integer(IDADEMAE),         
    idadpai = as.integer(IDADEPAI),             
    codmunre = as.integer(CODMUNRES),          
    
    qtgestant = as.integer(QTDGESTANT),         
    qtpartnor = as.integer(QTDPARTNOR),      
    qtpartces = as.integer(QTDPARTCES),      
    qtfilvivo = as.integer(QTDFILVIVO),      
    qtfilmot = as.integer(QTDFILMORT),      
    
    codmunasc = as.integer(CODMUNNASC),    
    tpparto = PARTO,        
    conspre = CONSULTAS,   
    
    tpgravidez = GRAVIDEZ,    
    mesprent = as.integer(MESPRENAT),          
    
    sexo = SEXO,        
    
    idanomal_Sim = ifelse(IDANOMAL == 1, 1,0),  
    idanomal_Nao = ifelse(IDANOMAL == 2, 1,0),  
    idanomal_Ign = ifelse(IDANOMAL == 9, 1,0)   
    
  ) %>%
  select(ano_ref, estcv, idadmae , idadpai, codmunre, 
         qtgestant, qtpartnor, qtpartces, qtfilvivo, qtfilmot, 
         codmunasc, tpparto, conspre, 
         tpgravidez, mesprent, sexo,
         idanomal_Sim, idanomal_Nao, idanomal_Ign)

# Unificando a base ------------------------------------------------------------

uni1 <- union_all(sinasc_15_bs, sinasc_16_bs)
uni2 <- union_all(sinasc_17_bs, sinasc_18_bs)
uni3 <- union_all(uni2, sinasc_19_bs)
sinasc <- union_all(uni1,uni3)

sinasc %>% 
  sdf_ncol()
sinasc %>% 
  sdf_nrow()

sinasc_bs <- sinasc %>% sdf_repartition(partitions=1)
spark_write_csv(sinasc_bs, 
                path = "Base_de_Dados\\sinasc_bs.csv",
                header = TRUE,
                delimiter = ",")


spark_disconnect(sc)
