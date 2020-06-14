IBim python backend developer test tasks
=======

Solutions for Bim python backend developer test tasks by Vasilev Viktor. Used Python 3.8.3


Set it up
------

1 Clone github repository with solutions and go to the root directory

    $ git clone https://github.com/v-v-d/ibim.git
    $ cd ibim
    
2 Get python venv module if it's not exists

    $ sudo apt-get install python3-venv
    
3 Create a virtual environment, update pip and install the requirements

    $ python3 -m venv venv
    $ source venv/bin/activate
    $ pip install --upgrade pip
    $ pip install -r requirements.txt
    
4 Run it. Files .xlsx with processed data will appear in the ibim/result folder

    $ python data_analyser.py
