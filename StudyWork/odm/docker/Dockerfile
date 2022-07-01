# begin with stable r version 
FROM rocker/tidyverse:4.1.3

#Set the working directory to be used when the docker gets run
# WORKDIR /usr

# install aws-cli
RUN apt-get update
RUN apt install -y awscli # -y required for prompts

# install r libraries
# use specific versions to ensure consistency
# call runs forever without any download occurring
#RUN Rscript -e "library(remotes)"
#RUN Rscript -e "remotes::install_version('arrow', version = '8.0.0', repos = 'http://cran.us.r-project.org')"
#RUN Rscript -e "remotes::install_version('xml2', version = '1.3.3', repos = 'http://cran.us.r-project.org')"
#RUN Rscript -e "remotes::install_version('aws.s3', version = '0.3.21', repos = 'http://cran.us.r-project.org')"
#RUN Rscript -e "remotes::install_version('R5', version = '2.5.1', repos = 'http://cran.us.r-project.org')"


# currently installations are hanging
#RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
#RUN Rscript -e "install.packages('arrow')"
#RUN Rscript -e "install.packages('R6')"
#RUN Rscript -e "install.packages('xml2')"
#RUN Rscript -e "install.packages('aws.s3')"





