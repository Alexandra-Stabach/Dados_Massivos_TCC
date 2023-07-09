REP=100       #número de repetições
N=100000      #tamanho da amostra
n.mods <- 10  #número de modelos (modelagem 3) 

set.seed(3)

ACC1 <- ACC2 <- ACC3  <- numeric(0)
COEF1 <- COEF2 <- COEF3  <- matrix(0,nrow=REP,ncol=7)
PV1 <- PV2 <- PV3  <- matrix(0,nrow=REP,ncol=7)

for (ii in 1:REP){
  
  x1 <-  rnorm(N)        
  x2 <-  rnorm(N)
  x3 <-  rnorm(N)
  z = 1 + 1*x1 + 1*x2 + 1*x3       
  pr = 1/(1+exp(-z))         
  y = rbinom(N,1,pr) 
  u1 <-  runif(N)        
  u2 <-  runif(N)
  u3 <-  runif(N)
  
  dados= data.frame(y,x1,x2,x3,u1,u2,u3)
  
  p.trein=0.7
  
  l <- sample(1:nrow(dados),nrow(dados)*p.trein)
  dados.trein <- dados[ l,]
  dados.teste <- dados[-l,]
  
  #modelo 1 - full
  
  mod1 <- glm(y~.,binomial(link = "logit"),
              data = dados.trein)
  summary(mod1)
  
  
  #modelo 2 - amostra 10  mil
  l.sample <- sample(1:nrow(dados.trein),size=10000)
  dados.sample <- dados.trein[l.sample,]
  
  mod2 <- glm(y~.,binomial(link = "logit"),
              data = dados.sample)
  summary(mod2)
  
  
  #modelo 3 - 10 amostras de mil
  mods <- list()
  preds <- matrix(0,nrow=nrow(dados.trein),ncol=n.mods)
  for(i in 1:n.mods){
    l.sample <- sample(1:nrow(dados.trein),size=1000)
    dados.sample.i <- dados[l.sample,]
    
    mods[[i]] <- glm(y~.,binomial(link = "logit"),
                     data = dados.sample.i)
  }
  
  for(i in 1:n.mods){
    p=predict(mods[[i]],dados.trein,type='response')
    d=rep(0,length(p))
    d[p>0.5]=1
    preds[,i]=d
  }
  
  dados.filt <- dados.trein[(apply(preds,1,sd))!=0,]
  nrow(dados.filt)
  
  mod3 <- glm(y~.,binomial(link = "logit"),
              data = dados.filt)
  
  
  COEF1[ii,] <- summary(mod1)$coef[,1]
  COEF2[ii,] <- summary(mod2)$coef[,1]
  COEF3[ii,] <- summary(mod3)$coef[,1]
  
  PV1[ii,] <- summary(mod1)$coefficients[,4]
  PV2[ii,] <- summary(mod2)$coefficients[,4]
  PV3[ii,] <- summary(mod3)$coefficients[,4]
  
  
  Y1=predict(mod1,dados.teste,type="response")>0.5
  Y2=predict(mod2,dados.teste,type="response")>0.5
  Y3=predict(mod3,dados.teste,type="response")>0.5
  
  
  m1 <- table(Y1+1,dados.teste$y)
  m2 <- table(Y2+1,dados.teste$y)
  m3 <- table(Y3+1,dados.teste$y)
  
  
  ACC1[ii] <- sum(diag(m1))/sum(m1)
  ACC2[ii] <- sum(diag(m2))/sum(m2)
  ACC3[ii] <- sum(diag(m3))/sum(m3)
  
  print(ii)
}


plot(dados[1:1000,-1],col=dados[1:1000,1]+1)

boxplot(ACC1,ACC2,ACC3,
        ylim=c(
          min(c(ACC1,ACC2,ACC3)),
          max(c(ACC1,ACC2,ACC3))))
abline(h=mean(c(ACC1,ACC2,ACC3)),col="red",lwd=3)

par(mfrow = c(1,3))

boxplot(COEF1,
        ylim=c(
          min(c(COEF1,COEF2,COEF3)),
          max(c(COEF1,COEF2,COEF3))))
points(1:7,c(1,1,1,1,0,0,0),pch=19,col="tomato")

boxplot(COEF2,
        ylim=c(
          min(c(COEF1,COEF2,COEF3)),
          max(c(COEF1,COEF2,COEF3))))

points(1:7,c(1,1,1,1,0,0,0),pch=19,col="tomato")
boxplot(COEF3,
        ylim=c(
          min(c(COEF1,COEF2,COEF3)),
          max(c(COEF1,COEF2,COEF3))))
points(1:7,c(1,1,1,1,0,0,0),pch=19,col="tomato")


boxplot(PV1,
        ylim=c(
          min(c(PV1,PV2,PV3)),
          max(c(PV1,PV2,PV3))))
abline(h=0.05,col="tomato",lwd=2)

pvr1=c(
  sum(PV1[,5]<0.05)/REP,
  sum(PV1[,6]<0.05)/REP,
  sum(PV1[,7]<0.05)/REP)

boxplot(PV2,
        ylim=c(
          min(c(PV1,PV2,PV3)),
          max(c(PV1,PV2,PV3))))
abline(h=0.05,col="tomato",lwd=2)

pvr2=c(
  sum(PV2[,5]<0.05)/REP,
  sum(PV2[,6]<0.05)/REP,
  sum(PV2[,7]<0.05)/REP)

boxplot(PV3,
        ylim=c(
          min(c(PV1,PV2,PV3)),
          max(c(PV1,PV2,PV3))))
abline(h=0.05,col="tomato",lwd=2)

pvr3=c(
  sum(PV3[,5]<0.05)/REP,
  sum(PV3[,6]<0.05)/REP,
  sum(PV3[,7]<0.05)/REP)

rbind(pvr1,pvr2,pvr3)