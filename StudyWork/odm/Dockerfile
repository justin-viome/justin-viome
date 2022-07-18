FROM rocker/tidyverse:4.1.2

WORKDIR /home

RUN apt-get -y update -qq \ 
  && apt-get install -y --no-install-recommends \
    awscli

RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile \
  && R -e "install.packages('xml2')" \ 
  && R -e "install.packages('aws.s3')" \ 
  && R -e "install.packages('R6')" \
  && R -e "install.packages('XML')"

# For a faster, more complete installation, set the environment variable NOT_CRAN=true before installing
ENV NOT_CRAN=true
RUN R -e "install.packages('arrow')" 

COPY odmparquet odmparquet
RUN R CMD INSTALL --preclean --no-multiarch --with-keep.source odmparquet

COPY scripts scripts

ENV S3_ODM_LOCATION=s3://viome-studies/v128.234/ODM/V128_Pilot_odm_export_20220601012550.xml
ENV S3_ODM_STUDY_NAME=v128.234

ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=

ENTRYPOINT ["Rscript", "scripts/generateParquetFromODM.R"]

# To build, `docker build -t odmparquet .`
# To run, `docker run --env AWS_ACCESS_KEY_ID= --env AWS_SECRET_ACCESS_KEY= odmparquet`