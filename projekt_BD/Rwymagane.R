wymagane_pakiety <- c("DBI","RSQLite","ggplot2","knitr","rmarkdown")
już_pobrane <- rownames(installed.packages())
do_pobrania <- setdiff(wymagane_pakiety,już_pobrane)
if(length(do_pobrania)>0){
  install.packages(do_pobrania,dependencies=TRUE)
}