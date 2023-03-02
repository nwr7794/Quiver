local laptop development

install the required dependencies with pip install -r requirements.txt 
confirm you have your API key as an environment variable export FRED_API_KEY="ABCXYZ"
run python app.py
open this URL in your browser http://localhost:3000/ and you should see “OK” confirming the app is running

i would consider the heroku environment a way to share progress and get reviews, it’ll be quicker and easier to develop the code when it’s running on your laptop. plus the local laptop version has hot reload (any time you save the app.py file it’ll reload the server so you can test)

Heroku DevOps Setup
the default branch in the Quiver project is now main and you’ll need to pull the repo to get that branch down
i added a branch protection rule to main that requires a pull request be created and approved before changes can be merged 
(git overview and PR info here https://www.atlassian.com/git)
TLDR: incremental change review before merging into the main branch

heroku automatically deploys the main branch whenever there are new commits added, meaning they have been reviewed and approved

i enabled “review environments” on heroku, which creates a new copy of the app every time a pull request is created

this means you could make a small change, put up a PR, and then send me the URL to test it out as i review it. amazing that heroku gives that to you in 2 clicks of setup

i also created long running environments for each of us 
this means that any commits you add to the noah branch will be deployed automatically. there is no branch protection rule here, the idea is any changes you want to put up on heroku can be pushed immediately, without changing my copy of the app

https://quiver-john.herokuapp.com/
https://quiver-noah.herokuapp.com/

Once you have changes you want me to review, put up a PR to merge your branch into main, and that will spin up a new environment for the review. then you can keep developing on noah for the next changeset