TODO:

- should all meetings in api, should indexed status on frontend
- upsert with trasncript (later should be doing with cache)




TODO 1
to consider:

- we many many to many relations users to meetings yet only one surrary, though it's based on general context of a user. Need to dscide
- we may switch to one cummspy per user 
-- more tokens uses
-- database change

- we may go for one summary on sumget of union of contet of users
-- may enchance output quality
-- will lead to data leakage

for now just take first user and delivering from it