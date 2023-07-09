### Bibliotecas ----------------------------------------------------------------
library(tidyverse)
library(sparklyr)
library(geobr) # Malha territorial -> Mapa
library(hrbrthemes) # grafico de barras
library(corrr)
library(dbplot)
library(pROC)
library(stats)

# Diretorio --------------------------------------------------------------------
setwd("C:\\Users\\joao9\\OneDrive\\Documentos\\GitHub\\TCC_Dados_Massivos\\")

# Conexão ----------------------------------------------------------------------
sc <- spark_connect(master = "local")

# Importando da base tratada
sinasc <- spark_read_csv(sc, "sinasc_bs", 
                         "Base_de_Dados\\sinasc_bs.csv\\part-00000-e80f01b6-71b7-4554-8fba-83c5a3ff82d1-c000.csv",
                         delim = ",", memory = FALSE)

## -- Dimensão
sinasc %>% 
  sdf_ncol()
colnames(sinasc)
sinasc %>% 
  sdf_nrow()
## --

### Análise e filtros ----------------------------------------------------------

## Verificando NA's

n.mis <- sinasc %>% 
  summarise_all(~ sum(as.integer(is.na(.)))) %>% 
  collect()

n <- sinasc %>% count() %>% 
  collect() %>% as.numeric()

n.mis/n

# Desconsiderando linhas com NA's exerto para idadpai
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

sdf_describe(sinasc_fil, cols = c("idadmae","idadpai", "qtgestant","qtfilvivo","qtfilmot","qtpartnor","qtpartces"))

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
    fx_idademae_0 = ifelse(idadmae < 10, 0,
                           ifelse(idadmae >= 10 & idadmae < 20, 1,
                                  ifelse(idadmae >= 20 & idadmae < 30, 2,
                                         ifelse(idadmae >= 30 & idadmae < 40, 3,
                                                ifelse(idadmae >= 40 & idadmae < 50, 4,
                                                       ifelse(idadmae >= 50 & idadmae < 60, 5,
                                                              ifelse(idadmae >= 60, 6,
                                                                     ifelse(is.na(idadmae), 7,7))))))))
  )

bs_sinasc <- bs_sinasc %>%
  arrange(idadmae) %>%
  mutate(
    fx_idademae = ifelse(idadmae < 18, 0,
                         ifelse(idadmae >= 18 & idadmae < 30, 1,
                                ifelse(idadmae >= 30 & idadmae < 40, 2,
                                       ifelse(idadmae >= 40 & idadmae < 50, 3,
                                              ifelse(idadmae >= 50 , 4, idadmae)))))
  )

# Adiciona faixa de idade do pai
bs_sinasc <- bs_sinasc %>%
  arrange(idadpai) %>%
  mutate(
    fx_idadepai_0 = ifelse(idadpai < 10, 0,
                           ifelse(idadpai >= 10 & idadpai < 20, 1,
                                  ifelse(idadpai >= 20 & idadpai < 30, 2,
                                         ifelse(idadpai >= 30 & idadpai < 40, 3,
                                                ifelse(idadpai >= 40 & idadpai < 50, 4,
                                                       ifelse(idadpai >= 50 & idadpai < 60, 5,
                                                              ifelse(idadpai >= 60 & idadpai < 70, 6,
                                                                     ifelse(idadpai >= 70 & idadpai <80, 7,
                                                                            ifelse(idadpai >= 80 & idadpai <= 90, 8,
                                                                                   ifelse(idadpai >= 90 & idadpai <= 99, 9, idadpai))))))))))
  )

bs_sinasc <- bs_sinasc %>%
  mutate(
    fx_idadepai_0 = ifelse(is.na(fx_idadepai_0),10,fx_idadepai_0)
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

# Verificando se o nº de partos condiz com o nº de gestações
# view(bs_sinasc %>%
#        group_by(qtgestant) %>%
#        tally())
# 
# view(bs_sinasc %>%
#        group_by(qtpartnor) %>%
#        tally())
# 
# view(bs_sinasc %>%
#        group_by(qtpartces) %>%
#        tally())

bs_sinasc <- bs_sinasc %>%
  mutate(
    ind_gest = ifelse(qtpartnor + qtpartces <= qtgestant, 1, 0)
  )

bs_sinasc %>%
  group_by(ind_gest) %>%
  tally()

bs_sinasc <- bs_sinasc %>%
  filter(ind_gest == 1)

# Verificando se o estado de residencia e de nascimento é o mesmo e se a proporção de casos é diferente em caso de mudança
bs_sinasc <- bs_sinasc %>%
  mutate(
    ind_mudanca = ifelse(codmunre != codmunasc, 1, 0)
  )

bs_sinasc %>%
  group_by(ind_mudanca) %>%
  tally() 

bs_sinasc %>%
  filter(idanomal_Ign == 0) %>%
  sdf_pivot(ind_mudanca ~ idanomal_Sim, fun.aggregate = "count")

#===============================================================================
# Gráicos e tabelas ------------------------------------------------------------

## Por idade -------------------------------------------------------------------
### Mãe ------------------------------------------------------------------------
cs_id_mae_fx_0 <- bs_sinasc %>% 
  group_by(fx_idademae_0) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_id_m_fx_0 <- cs_id_mae_fx_0 %>%
  summarise(Faixa = ifelse(fx_idademae_0 == 0, "[0-10)",
                           ifelse(fx_idademae_0 == 1, "[10-20)",
                                  ifelse(fx_idademae_0 == 2, "[20-30)",
                                         ifelse(fx_idademae_0 == 3, "[30-40)",
                                                ifelse(fx_idademae_0 == 4, "[40-50)",
                                                       ifelse(fx_idademae_0 == 5, "[50-60)",
                                                              ifelse(fx_idademae_0 == 6, "[60,100)", "Ignorado"))))))),
            nascimentos = nascimentos,
            num_casos = casos,
            proporcao_casos = proporcao
  )

tab_fx_id_m_0 <- tab_id_m_fx_0 %>% arrange(Faixa)

graf_id_m_fx_0 <- ggplot(tab_fx_id_m_0, aes(x=Faixa, y=proporcao_casos, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.05))+
  theme_light()+
  labs(x="Faixa idade da mãe", y="Proporção de casos")+
  geom_text(aes(x=Faixa, y=0),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(0.5),
            vjust = 1) +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))
#---

cs_id_mae_fx <- bs_sinasc %>% 
  group_by(fx_idademae) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_id_m_fx <- cs_id_mae_fx %>%
  summarise(Faixa = ifelse(fx_idademae == 0, "[0-18)",
                           ifelse(fx_idademae == 1, "[18-30)",
                                  ifelse(fx_idademae == 2, "[30-40)",
                                         ifelse(fx_idademae == 3, "[40-50)",
                                                ifelse(fx_idademae == 4, "[50-100)", "Ignorado"))))),
            nascimentos = nascimentos,
            num_casos = casos,
            proporcao_casos = proporcao
  )

tab_fx_id_m <- tab_id_m_fx %>% arrange(Faixa)

graf_id_m_fx <- ggplot(tab_fx_id_m, aes(x=Faixa, y=proporcao_casos, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.05))+
  theme_light()+
  labs(x="Faixa idade da mãe", y="Proporção de casos")+
  geom_text(aes(x=Faixa, y=0),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(0.5),
            vjust = 1) +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))


### Pai ------------------------------------------------------------------------
cs_id_pai_fx_0 <- bs_sinasc  %>% 
  group_by(fx_idadepai_0) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_id_p_fx_0 <- cs_id_pai_fx_0 %>%
  summarise(Faixa = ifelse(fx_idadepai_0 == 0, "[0-10)",
                           ifelse(fx_idadepai_0 == 1, "[10-20)",
                                  ifelse(fx_idadepai_0 == 2, "[20-30)",
                                         ifelse(fx_idadepai_0 == 3, "[30-40)",
                                                ifelse(fx_idadepai_0 == 4, "[40-50)",
                                                       ifelse(fx_idadepai_0 == 5, "[50-60)",
                                                              ifelse(fx_idadepai_0 == 6, "[60-70)",
                                                                     ifelse(fx_idadepai_0 == 7, "[70-80)",
                                                                            ifelse(fx_idadepai_0 == 8, "[80-90)",
                                                                                   ifelse(fx_idadepai_0 == 9, "[90-99]",
                                                                                          "Ignorado")))))))))),
            nascimentos = nascimentos,
            num_casos = casos,
            proporcao_casos = proporcao
  )

tab_fx_id_p_0 <- tab_id_p_fx %>% arrange(Faixa)

graf_id_p_fx <- ggplot(tab_fx_id_p_0, aes(x=Faixa, y=proporcao_casos, label = nascimentos))+
  geom_bar(stat = "identity", fill = c("#f2f2f2", rep("#5dc299",10)), color= "#ffffff")+
  ylim(c(-0.0030,0.05))+
  theme_light()+
  labs(x="Faixa idade do pai", y="Proporção de casos")+
  geom_text(aes(x=Faixa, y=-0.003),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(0.5),
            vjust = -0.5) +
  theme(axis.text = element_text(size = 13),
        axis.title = element_text(size = 18))

#--
group_by(fx_idadepai) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_id_p_fx <- cs_id_pai_fx %>%
  summarise(Faixa = ifelse(fx_idadepai == 0, "[0-18)",
                           ifelse(fx_idadepai == 1, "[18-30)",
                                  ifelse(fx_idadepai == 2, "[30-40)",
                                         ifelse(fx_idadepai == 3, "[40-50)",
                                                ifelse(fx_idadepai == 4, "[50-99)", "Ignorado"))))),
            nascimentos = nascimentos,
            num_casos = casos,
            proporcao_casos = proporcao
  )

tab_fx_id_p <- tab_id_p_fx %>% arrange(Faixa)

graf_id_p_fx <- ggplot(tab_fx_id_p, aes(x=Faixa, y=proporcao_casos, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",4), "#f2f2f2"), color= "#ffffff")+
  ylim(c(-0.0030,0.05))+
  theme_light()+
  labs(x="Faixa idade do pai", y="Proporção de casos")+
  geom_text(aes(x=Faixa, y=-0.003),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(0.5),
            vjust = -0.5) +
  theme(axis.text = element_text(size = 13),
        axis.title = element_text(size = 18))

## Por estado civil da mãe -----------------------------------------------------

cs_est_civil <- bs_sinasc %>% 
  group_by(estcv) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_est_civil <- cs_est_civil %>%
  mutate(
    estado_civil = ifelse(estcv == 1,"Solteira",
                          ifelse(estcv == 2, "Casada",
                                 ifelse(estcv == 3, "Viúva",
                                        ifelse(estcv == 4, "Separada",
                                               ifelse(estcv == 5, "União estável",
                                                      ifelse(estcv == 9, "Ignorado","NA"))))))
  ) %>% arrange(factor(estado_civil, levels = c("Solteira", "Casada", "Viúva",
                                                "Separada", "União estável", "Ignorado")))

graf_est_civil <- ggplot(tab_est_civil, aes(x=factor(estado_civil, levels = c("Solteira", "Casada", "Viúva",
                                                                              "Separada", "União estável", "Ignorado")),
                                            y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",5),"#f2f2f2"), color= "#ffffff")+
  ylim(c(-0.0009,0.018))+
  theme_light()+
  labs(x="Estado civil da mãe", y="Proporção de casos")+
  geom_text(aes(x=estado_civil, y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

## Quantidade de gestações -----------------------------------------------------

cs_qt_gestações <- bs_sinasc %>% 
  group_by(qtgestant) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_qt_gestações <- cs_qt_gestações %>%
  arrange(qtgestant)

graf_qt_gestações <- ggplot(tab_qt_gestações, aes(x=qtgestant, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.028))+
  coord_flip()+
  theme_light()+
  labs(x="Número de gestações anteriores", y="Proporção de casos")+
  geom_text(colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            hjust = -0.2)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18)) 

## Pelo quantidade de partos normais -------------------------------------------

cs_part_norm <- bs_sinasc %>% 
  group_by(qtpartnor) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>%
  collect()

tab_part_norm <- cs_part_norm %>%
  arrange(qtpartnor)

graf_part_norm <- ggplot(tab_part_norm, aes(x=qtpartnor, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.028))+
  theme_light()+
  coord_flip()+
  labs(x="Quantidade de partos normais", y="Proporção de casos")+
  geom_text(colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            hjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

## Pela quantidade de partos cesarianos ----------------------------------------

cs_part_ces <- bs_sinasc %>% 
  group_by(qtpartces) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_part_ces <- cs_part_ces %>%
  arrange(qtpartces)

graf_part_ces <- ggplot(tab_part_ces, aes(x=qtpartces, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.028))+
  theme_light()+
  coord_flip()+
  labs(x="Quantidade de partos cesários", y="Proporção de casos")+
  geom_text(colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            hjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

# Quantidade de filhos nascidos vivos ------------------------------------------
cs_qt_vivos <- bs_sinasc %>% 
  group_by(qtfilvivo) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_qt_vivos <- cs_qt_vivos %>%
  filter(qtfilvivo != 99) %>%
  arrange(qtfilvivo)

graf_qt_vivos <- ggplot(tab_qt_vivos, aes(x=qtfilvivo, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.028))+
  coord_flip()+
  theme_light()+
  labs(x="Número de filhos vivos", y="Proporção de casos")+
  geom_text(colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            hjust = -0.2)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

## Quantidade de filhos nascidos mortos/abortos --------------------------------

cs_qt_mortos <- bs_sinasc %>% 
  group_by(qtfilmot) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_qt_mortos <- cs_qt_mortos %>%
  filter(qtfilmot != 99) %>%
  arrange(qtfilmot)

graf_qt_mortos <- ggplot(tab_qt_mortos, aes(x=qtfilmot, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(0,0.028))+
  coord_flip()+
  theme_light()+
  labs(x="Número de filhos mortos", y="Proporção de casos")+
  geom_text(colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            hjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

## Pelo "Mês de inicio do pré-natal --------------------------------------------

cs_mes_pre <- bs_sinasc %>% 
  group_by(mesprent) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_mes_pre <- cs_mes_pre %>%
  filter(mesprent != 99)

graf_mes_pre <- ggplot(tab_mes_pre, aes(x=mesprent, y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(-0.001,0.018))+
  theme_light()+
  labs(x="Mês de inicio do pré-natal", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

# Pelo nº de consultas pré natal -----------------------------------------------

cs_conspre <- bs_sinasc %>% 
  group_by(conspre) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_conspre <- cs_conspre %>%
  mutate(
    consultas_pre = ifelse(conspre == 1,"Nenhuma",
                           ifelse(conspre == 2, "1 a 3",
                                  ifelse(conspre == 3, "4 a 6",
                                         ifelse(conspre == 4, "7 ou mais",
                                                ifelse(conspre == 9, "Ignorado","NA")))))
  )%>%
  arrange(factor(consultas_pre, levels = c("Nenhuma", "1 a 3", "4 a 6","7 ou mais","Ignorado")))

graf_conspre <- ggplot(tab_conspre, aes(x=factor(consultas_pre, levels = c("Nenhuma", "1 a 3", "4 a 6","7 ou mais","Ignorado")), 
                                        y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",4),"#f2f2f2"), color= "#ffffff")+
  ylim(c(-0.001,0.018))+
  theme_light()+
  labs(x="Número de consultas pré-natal", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))


## Pelo tipo de gravidez -------------------------------------------------------

cs_tpgravidez <- bs_sinasc %>% 
  group_by(tpgravidez) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_tpgravidez <- cs_tpgravidez %>%
  mutate(
    tp_gravidez = ifelse(tpgravidez == 1,"Unica",
                         ifelse(tpgravidez == 2, "Dupla",
                                ifelse(tpgravidez == 3, "Tripla ou mais",
                                       ifelse(tpgravidez == 9, "Ignorado","NA"))))
  ) %>%
  arrange(factor(tp_gravidez, levels = c("Unica", "Dupla", "Tripla ou mais","Ignorado")))
view(tab_tpgravidez)

graf_tp_gravidez <- ggplot(tab_tpgravidez, aes(x=factor(tp_gravidez, levels = c("Unica", "Dupla", "Tripla ou mais","Ignorado")), 
                                               y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",3),"#f2f2f2"), color= "#ffffff")+
  ylim(c(-0.001,0.018))+
  theme_light()+
  labs(x="Tipo de gravidez", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

# Por tipo de parto ------------------------------------------------------------
cs_tp_parto <- bs_sinasc %>% 
  group_by(tpparto) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_tp_parto <- cs_tp_parto %>%
  mutate(
    tipo_parto = ifelse(tpparto == 1,"Vaginal",
                        ifelse(tpparto == 2, "Cesário",
                               ifelse(tpparto == 9, "Ignorado","NA")))
  ) %>%
  arrange(factor(tipo_parto, levels = c("Vaginal", "Cesário", "Ignorado")))
view(tab_tp_parto)

graf_tp_parto <- ggplot(tab_tp_parto, aes(x=factor(tipo_parto, levels = c("Vaginal", "Cesário", "Ignorado")), 
                                          y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",2),"#f2f2f2"), color= "#ffffff")+
  ylim(c(-0.001,0.018))+
  theme_light()+
  labs(x="Tipo de parto", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

# Pelo sexo do bebê ------------------------------------------------------------

cs_sexo <- bs_sinasc %>% 
  group_by(sexo) %>% 
  summarise(nascimentos = sum(idanomal_Sim) + sum(idanomal_Nao),
            casos = sum(idanomal_Sim),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3)) %>% 
  collect()

tab_sexo <- cs_sexo %>%
  mutate(
    sexo_bebe = ifelse(sexo == 1,"Masculino",
                       ifelse(sexo == 2, "Feminino",
                              ifelse(sexo == 0, "Ignorado","NA")))
  ) %>%
  arrange(factor(sexo_bebe, levels = c("Masculino", "Feminino","Ignorado")))


graf_sexo_ign <- ggplot(tab_sexo, aes(x=factor(x=factor(sexo_bebe, levels = c("Masculino", "Feminino","Ignorado")), levels = c("Masculino", "Feminino","Ignorado")),
                                      y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = c(rep("#5dc299",2),"#f2f2f2"), color= "#ffffff")+
  theme_light()+
  labs(x="Sexo do recém-nascido", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 18))

tb_sexo <- tab_sexo %>%
  filter(sexo_bebe != "Ignorado")

grf_sexo <- ggplot(tb_sexo, aes(x=factor(x=factor(sexo_bebe, levels = c("Masculino", "Feminino","Ignorado")), levels = c("Masculino", "Feminino","Ignorado")),
                                y=proporcao, label = nascimentos))+
  geom_bar(stat = "identity", fill = "#5dc299", color= "#ffffff")+
  ylim(c(-0.001,0.018))+
  theme_light()+
  labs(x="Sexo do recém-nascido", y="Proporção de casos")+
  geom_text(aes(y=-0.0009),
            colour = "#154531", fontface = "bold", 
            position = position_dodge(1),
            vjust = -0.3)+
  theme(axis.text = element_text(size = 12),
        axis.title = element_text(size = 16))

## casos por Região  ----------------------------------------------------------

cs_regres <- bs_sinasc %>% 
  group_by(codestres) %>% 
  summarise(casos = sum(idanomal_Sim),
            nascimento_sem = sum(idanomal_Nao),
            nascimento_igno = sum(idanomal_Ign),
            proporcao = round(sum(idanomal_Sim)/(sum(idanomal_Sim) + sum(idanomal_Nao)),3))%>% 
  collect()
view(cs_regres)

estados <- read_state(year = 2019)
estados$codestado <- as.character(estados$code_state)

cs_regres <- cs_regres %>%
  mutate(codestado = codestres) %>%
  select(codestado, casos, nascimento_sem, nascimento_igno, proporcao)

est_cs_res <- dplyr::left_join(estados, cs_regres, by= "codestado")

mapa_cs_reg <- ggplot(data=est_cs_res)+
  geom_sf(aes(fill=proporcao),colour  = "#256357") +
  scale_fill_gradient(low = "#e6fcf3", high = "#0c4a31", name = "Proporção") +
  theme_bw()

casos_regiao <- est_cs_res %>% 
  group_by(codestado) %>% 
  summarise(estado = name_state,
            nascimentos = sum(casos,nascimento_sem),
            num_casos = casos,
            proporcao_casos = proporcao
  ) %>% 
  collect()

# ------------------------------------------------------------------------------
# Filtros ignorados ------------------------------------------------------------
sinasc_bs <- bs_sinasc %>%
  filter(qtfilvivo != 99) %>%
  filter(qtfilmot != 99) %>%
  filter(mesprent != 99) %>%
  filter(idanomal_Ign == 0)

sinasc_bs %>% sdf_nrow()

# Correlação -------------------------------------------------------------------

correlacoes <- sinasc_bs %>% select(idadmae, idadpai,
                                    qtgestant,
                                    qtfilvivo, qtfilmot,
                                    qtpartnor, qtpartces) %>%
  na.omit()%>%
  correlate(method = "spearman")

spark_disconnect()