FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Validator"

#install shapely for client side filtering
#RUN pip install shapely

USER ops
# copy packages
COPY . /home/ops/verdi/ops/standard_product_validator
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product_validator

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
