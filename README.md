# IndeedAssignment
Assignment Challenge for Indeed Data Engineer

## Install and Run

* Go to your workspace: 
  * `cd ~/myWorkSpace`
* Create a project folder: 
  * `mkdir MyNewSuperProject && cd MyNewSuperProject`
* Clone the lib repo: 
  * `git clone https://github.com/The-Brains/CommonLib.git`
* Clone this repo: 
  * `git clone https://github.com/The-Brains/IndeedAssignment.git`
* `cd IndeedAssignment`
* `sbt`
* `compile`
* `run`
* When it is all done: 
  * `exit`
* Go see the results: 
  * `cd target/Output/test_salaries_2013-03-07.csv`
* `open *.csv`

## All in one

If you want to copy / paste all the commands: 

**DO NOT FORGET TO CHANGE THE FOLDER NAME**
```
rootFolder="MyWorkPlace" &&\
    projectFolder="MySuperProject" &&\
    cd ~/${rootFolder} &&\
    mkdir ${projectFolder} &&\
    cd ${projectFolder} &&\
    git clone https://github.com/The-Brains/CommonLib.git &&\
    git clone https://github.com/The-Brains/IndeedAssignment.git &&\
    cd IndeedAssignment &&\
    sbt
```

and then `run`