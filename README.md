# Movie Recommender System   BY   [![N|Solid](https://think-big.solutions/img/logo.png)](https://think-big.solutions)

This tutorial will guide you to build movie recommender engine using collaborative filtering with [Spark's Alternating Least Saqures ](https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html) 
## This tutorial consist of python app and jupyter notebbok

## python app 
  ### python app contains the engine.py , server.py and app.py </br >
  - >**server.py**   -> contains cherry server configurations </br >
  - >**app.py** 	-> contains app end-points </br >
  - >**engine.py**   -> contains core engine </br >
  ## Needed packages for python app 
  - > CherryPy
  - > PasteScript
  - > flask 
    you can setting your environment as follow 
    ### To create new virtualenv 
    ```bash
        conda create -n myenv python=3.5
      ```
     ### To activate your virtual environment 
    ```bash
        source activate myenv
     ```
    ### To install need packages 
    ~~~bash
    conda install cherrypy
    ~~~
    
    ### To install flask 
    ~~~bash
    conda install flask
    ~~~
    
    ### To install pastescript 
    ~~~bash
    conda install pastescript
    ~~~
    
    ### To install jupyter 
    ```bash
    conda install jupyter
    ```
    
   ### To open jupyter notebook 
   ```bash 
   jupyter notebook
   ```
   
   ### To run server 
   ```
   $SPARK_HOME/bin/spark-submit server.py
   ```
   
   
  ## APP endpoints At **0.0.0.0:5432**
 
   
