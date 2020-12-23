FROM jupyter/minimal-notebook:latest AS nb-arcgis

RUN conda install -c esri arcgis -y \
    && conda install -c conda-forge python-dotenv \
    && pip install boto3

RUN git clone https://github.com/knu2xs/safegraph-data-utilities

RUN pip install -e /home/jovyan/safegraph-data-utilities/.

EXPOSE 8888

ENTRYPOINT ["/usr/local/bin/start.sh", "jupyter", "lab", \
                "--NotebookApp.password=sha1:3f2184d5fc9b:a7ed411f8dc0c5ce50f90f51fa67ff7e01f5482a", \
                "--NotebookApp.allow_remote_access=1"]