FROM rocker/tidyverse:4.1.2

WORKDIR /home

RUN apt-get -y update -qq \ 
  && apt-get install -y --no-install-recommends \
    awscli

RUN Rscript -e "library(remotes)"
RUN Rscript -e "remotes::install_version('xml2', version = '1.3.3', repos = 'http://cran.us.r-project.org')"
RUN Rscript -e "remotes::install_version('aws.s3', version = '0.3.21', repos = 'http://cran.us.r-project.org')"
RUN Rscript -e "remotes::install_version('R6', version = '2.5.1', repos = 'http://cran.us.r-project.org')"
RUN Rscript -e "remotes::install_version('XML', version = '3.99.0.9', repos = 'http://cran.us.r-project.org')"

# For a faster, more complete installation, set the environment variable NOT_CRAN=true before installing
ENV NOT_CRAN=true
RUN Rscript -e "remotes::install_version('arrow', version = '8.0.0', repos = 'http://cran.us.r-project.org')"

COPY odmparquet odmparquet
RUN R CMD INSTALL --preclean --no-multiarch --with-keep.source odmparquet

COPY scripts scripts

ENV S3_ODM_LOCATION=s3://viome-studies/v128.234/ODM/V128_Pilot_odm_export_20220601012550.xml
ENV S3_ODM_STUDY_NAME=v128.234-test

ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=

ENTRYPOINT ["Rscript", "scripts/generateParquetFromODM.R"]

# To build, `docker build -t odmparquet .`
# To run, `docker run --env AWS_ACCESS_KEY_ID= --env AWS_SECRET_ACCESS_KEY= odmparquet`