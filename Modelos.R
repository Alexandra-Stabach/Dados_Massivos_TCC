### Bibliotecas --------------------------------------------------------------------
library(tidyverse)
library(sparklyr)
library(pROC)

# Diretorio --------------------------------------------------------------------
setwd("C:\\Users\\joao9\\OneDrive\\Documentos\\GitHub\\TCC_Dados_Massivos\\")

# Conexão ----------------------------------------------------------------------
sc <- spark_connect(master = "local")

# Importando da base tratada
sinasc <- spark_read_csv(sc, "sinasc_bs", 
                         "Base_de_Dados\\sinasc_bs.csv\\part-00000-e80f01b6-71b7-4554-8fba-83c5a3ff82d1-c000.csv",
                         delim = ",", memory = FALSE)

### --------------------------------------------------------------------------------
glimpse(sinasc)
# desconsiderando linhas com NA's exerto em idadpai
sinasc_fil <- sinasc %>% 
  filter(!is.na(idadmae))%>%
  filter(!is.na(estcv)) %>%
  filter(!is.na(codmunre)) %>%
  filter(!is.na(qtgestant)) %>%
  filter(!is.na(qtpartnor)) %>%
  filter(!is.na(qtpartces)) %>%
  filter(!is.na(qtfilvivo)) %>%
  filter(!is.na(qtfilmot)) %>%
  filter(!is.na(codmunasc)) %>%
  filter(!is.na(tpparto)) %>%
  filter(!is.na(conspre)) %>%
  filter(!is.na(tpgravidez)) %>%
  filter(!is.na(mesprent)) %>%
  filter(!is.na(sexo)) %>%
  filter(!is.na(idanomal_Sim)) %>%
  filter(!is.na(idanomal_Nao)) %>%
  filter(!is.na(idanomal_Ign))

### --------------------------------------------------------------------------------
# Adiciona estado de nascimento
bs_sinasc <- sinasc_fil %>%
  arrange(codmunasc) %>%
  mutate(codestnas = stringr::str_sub(codmunasc, 1, 2))

# Adiciona estado de residencia
bs_sinasc <- bs_sinasc %>%
  arrange(codmunre) %>%
  mutate(codestres = stringr::str_sub(codmunre, 1, 2))

# Adiciona faixa de idade da mãe
bs_sinasc <- bs_sinasc %>%
  arrange(idadmae) %>%
  mutate(
    fx_idademae = ifelse(idadmae < 18, 0,
                         ifelse(idadmae >= 18 & idadmae < 30, 1,
                                ifelse(idadmae >= 30 & idadmae < 40, 2,
                                       ifelse(idadmae >= 40 & idadmae < 50, 3,
                                              ifelse(idadmae >= 50 , 4, idadmae)))))
  )

bs_sinasc <- bs_sinasc %>%
  arrange(idadpai) %>%
  mutate(
    fx_idadepai = ifelse(idadpai < 18, 0,
                         ifelse(idadpai >= 18 & idadpai < 30, 1,
                                ifelse(idadpai >= 30 & idadpai < 40, 2,
                                       ifelse(idadpai >= 40 & idadpai < 50, 3,
                                              ifelse(idadpai >= 50 , 4,NA)))))
  )

bs_sinasc <- bs_sinasc %>%
  mutate(
    fx_idadepai = ifelse(is.na(fx_idadepai),5,fx_idadepai)
  )

### --------------------------------------------------------------------------------
# Verificando se o nº de partos condiz com o nº de gestações
bs_sinasc <- bs_sinasc %>%
  mutate(
    ind_gest = ifelse(qtpartnor + qtpartces <= qtgestant, 1, 0)
  )

## Remove inconsistencias 
bs_sinasc <- bs_sinasc %>%
  filter(ind_gest == 1)

# Verificando se o estado de residencia e de nascimento é o mesmo e se a proporção de casos é diferente em caso de mudança
bs_sinasc <- bs_sinasc %>%
  mutate(
    ind_mudanca = ifelse(codmunre != codmunasc, 1, 0)
  )

### ----------------------------------------------------------------------------

# Filtro deterministico
sinasc_bs <- bs_sinasc %>%
  filter(sexo != 0)

# Filtros ignorados
sinasc_bs <- sinasc_bs %>%
  filter(qtfilvivo != 99) %>%
  filter(qtfilmot != 99) %>%
  filter(mesprent != 99) %>%
  filter(idanomal_Ign == 0) %>% 
  filter(tpgravidez!=9) %>%         ##categoria com baixa frequencia
  filter(fx_idademae!=4)         ##categoria com baixa frequencia

glimpse(sinasc_bs)


sinasc_bc <- sinasc_bs %>% select(idanomal_Sim,fx_idademae,
                                  tpgravidez, sexo, fx_idadepai,
                                  codestres, ind_mudanca, qtgestant,
                                  qtfilmot, conspre, mesprent)

glimpse(sinasc_bc)

sinasc_bc2 <- sinasc_bc %>% mutate(sexo=as.character(sexo),
                                   fx_idadepai=as.character(as.integer(fx_idadepai)),
                                   fx_idademae=as.character(as.integer(fx_idademae)),
                                   tpgravidez=as.character(as.integer(tpgravidez)),
                                   ind_mudanca=as.character(ind_mudanca),
                                   conspre=as.character(conspre))



glimpse(sinasc_bc2)

### --------------------------------------------------------------------------------
## Subamostragem (Undersampling)

dados1 <- sinasc_bc2 %>% filter(idanomal_Sim==1)
dados0 <- sinasc_bc2 %>% filter(idanomal_Sim==0) %>% sample_n(110281)

dados <- sdf_bind_rows(dados1,dados0)

dados %>% count()

particao <- dados %>% 
  sdf_random_split(training = 0.7, test = 0.3, seed = 2023)

treino <- particao$training 
teste <- particao$test 

### ===============================================================================
# MODELO 1 ------------------------------------------------------------------------

modelo_a1 <- ml_generalized_linear_regression(treino,
                                              idanomal_Sim ~ .,
                                              family = "binomial",
                                              link = "logit")

predicao_a1 <- ml_predict(modelo_a1, teste) 

### Matriz de confusão
predicao_a1 <- predicao_a1 %>%
  mutate(predicao_r = ifelse(prediction > 0.5,1,0))


# predicao_a1 %>%
#   group_by(idanomal_Sim) %>% summarise(MEAN=mean(prediction),
#                                        MIN=min(prediction),
#                                        MAX=max(prediction))

MC1 <- predicao_a1 %>% 
  sdf_crosstab("idanomal_Sim", "predicao_r") %>%
  collect()


area_roc_a1 <- ml_binary_classification_evaluator(predicao_a1,
                                                  label_col = "idanomal_Sim",
                                                  raw_prediction_col = "prediction",
                                                  metric_name = "areaUnderROC")


### ===============================================================================
# MODELO 2 ------------------------------------------------------------------------
# 1 modelo com uma amostra de 30 mil

set.seed(2306)
amostra <- treino %>% sample_n(30000) %>% collect()
teste_c <- teste %>% collect()

amostra <- amostra %>%
  mutate(fx_idademae = factor(fx_idademae, levels=0:4),
         fx_idadepai = factor(fx_idadepai, levels=0:5),
         tpgravidez = as.factor(tpgravidez),
         sexo = as.factor(sexo),
         codestres = as.factor(codestres),
         conspre = as.factor(conspre),
         ind_mudanca = as.factor(ind_mudanca)
  )

teste_c <- teste_c %>% 
  mutate(fx_idademae = factor(fx_idademae, levels=0:4),
         fx_idadepai = factor(fx_idadepai, levels=0:5),
         tpgravidez = as.factor(tpgravidez),
         sexo = as.factor(sexo),
         codestres = as.factor(codestres),
         conspre = as.factor(conspre),
         ind_mudanca = as.factor(ind_mudanca)
  )




modelo_a2 <- glm( idanomal_Sim ~ ., 
                  family = binomial(link = "logit"), data = amostra)

summary(modelo_a2)

probs_m2 <- predict(modelo_a2, teste_c, type="response")
predicao_a2 <- rep(0,nrow(teste_c))
predicao_a2[probs_m2>0.5]=1


area_roc_a2 <- round(auc(teste_c$idanomal_Sim,probs_m2),4)
roc_a2 <- roc(teste_c$idanomal_Sim,probs_m2)

MC2 <- table(teste_c$idanomal_Sim,predicao_a2)

ggroc(roc_a2, colour = "#5dc299", size= 1.5)+
  theme_light() +  
  geom_abline(intercept = 1, lty = "dashed") +
  annotate("text", x = 0.25, y = 0.25, label = paste(area_roc_a2), size=10, color = "#357058") +
  xlab("FPR") + ylab("TPR") +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))



### ===============================================================================
# MODELO 3 ------------------------------------------------------------------------
# 10 modelos de 3 mil obs

treino_r <- treino %>% collect()

mods <- list()
preds <- matrix(0,nrow=nrow(treino_r),ncol=10)
for(i in 1:10){
  l.sample <- sample(1:nrow(treino_r),size=3000)
  dados.sample.i <- treino_r[l.sample,]
  
  mods[[i]] <- glm(idanomal_Sim~.,binomial(link = "logit"),
                   data = dados.sample.i)
}

for(i in 1:10){
  p=predict(mods[[i]],treino_r,type='response')
  d=rep(0,length(p))
  d[p>0.5]=1
  preds[,i]=d
}

dados.filt <- treino_r[(apply(preds,1,sd))!=0,]
nrow(dados.filt)

modelo_a3 <- glm(idanomal_Sim~.,binomial(link = "logit"),
                 data = dados.filt)



summary(modelo_a3)

probs_m3 <- predict(modelo_a3, teste_c, type="response")
predicao_a3 <- rep(0,nrow(teste_c))
predicao_a3[probs_m3>0.5]=1


area_roc_a3 <- round(auc(teste_c$idanomal_Sim,probs_m3),4)
roc_a3 <- roc(teste_c$idanomal_Sim,probs_m3)

MC3 <- table(teste_c$idanomal_Sim,predicao_a3)

ggroc(roc_a3, colour = "#5dc299", size= 1.5)+
  theme_light() +  
  geom_abline(intercept = 1, lty = "dashed") +
  annotate("text", x = 0.25, y = 0.25, label = paste(area_roc_a3), size=10, color = "#357058") +
  xlab("FPR") + ylab("TPR") +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))


cbind(area_roc_a1,area_roc_a2,area_roc_a3)

spark_disconnect()
