# get shiny serves plus tidyverse packages image
FROM rocker/shiny-verse:latest

# system libraries of general use
RUN apt-get update && apt-get install -y \
    sudo \
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libproj-dev \
    libgeos-dev \
    libudunits2-dev \
    libgdal-dev 
  

# install R packages required 
# (change it dependeing on the packages you need)
RUN R -e "install.packages('shiny', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('shinythemes', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('shinyjs', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('lubridate', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('leaflet', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('leaflet.extras', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('mapview', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('mapedit', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('sp', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('sf', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('shinyWidgets', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('blastula', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('DBI', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('dplyr', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('plyr', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('rgdal', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('zip', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('RPostgres', repos='http://cran.rstudio.com/')"
RUN R -e "install.packages('lwgeom', repos='http://cran.rstudio.com/')"

# copy the app to the image
COPY /app /srv/shiny-server/

# select port
EXPOSE 3840

# allow permission
RUN sudo chown -R shiny:shiny /srv/shiny-server

# run app
CMD ["/usr/bin/shiny-server.sh"]