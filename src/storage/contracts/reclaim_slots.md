Every delete and the release check in their own transaction that the slot still
clears that session, so a repeated or late pass never touches a slot another
session took.
