library("sqldf")
library("dplyr")
library("data.table")
library("microbenchmark")

options(stringsAsFactors=FALSE)
Tags <- read.csv("travel_stackexchange_com/Tags.csv")
Badges <- read.csv("travel_stackexchange_com/Badges.csv")
Comments <- read.csv("travel_stackexchange_com/Comments.csv")
PostLinks <- read.csv("travel_stackexchange_com/PostLinks.csv")
Posts <- read.csv("travel_stackexchange_com/Posts.csv")
Users <- read.csv("travel_stackexchange_com/Users.csv")
Votes <- read.csv("travel_stackexchange_com/Votes.csv")
Tagstab <- data.table(Tags)
Badgestab <- data.table(Badges)
Commentstab <- data.table(Comments)
PostLinkstab <- data.table(PostLinks)
Poststab <- data.table(Posts)
Userstab <- data.table(Users)
Votestab <- data.table(Votes)
#1 
df_sql_1 <- function(df1){sqldf("SELECT Title, Score, ViewCount, FavoriteCount FROM df1 WHERE PostTypeId=1 AND FavoriteCount >= 25 AND ViewCount>=10000")}
df_base_1 <- function(df1){na.omit(df1[df1$PostTypeId == 1 & df1$FavoriteCount >= 25 & df1$ViewCount >= 10000, c("Title", "Score", "ViewCount", "FavoriteCount")])}
df_dplyr_1 <- function(df1){select(filter(df1, PostTypeId == 1 & FavoriteCount >= 25 & ViewCount >= 10000), c("Title", "Score", "ViewCount", "FavoriteCount"))}
df_table_1 <- function(df1){df1[PostTypeId == 1 & FavoriteCount >= 25 & ViewCount >= 10000, c("Title", "Score", "ViewCount", "FavoriteCount")]
}

#2
df_sql_2 <- function(df1, df2, df3){sqldf("SELECT df1.TagName, df1.Count, df2.OwnerUserId, df3.Age,
df3.Location, df3.DisplayName
FROM df1
JOIN df2 ON df2.Id=df1.WikiPostId
JOIN df3 ON df3.AccountId=df2.OwnerUserId
WHERE OwnerUserId != -1
ORDER BY Count DESC")}

df_base_2 <- function(df1, df2, df3){
a <- merge(merge(df1, df2, by.x="WikiPostId", by.y="Id"), df3, by.x ="OwnerUserId", by.y ="AccountId")
a <- a[a$OwnerUserId != -1, ]
a[order(a$Count, decreasing = TRUE, na.last = NA), c("TagName", "Count", "OwnerUserId", "Age", "Location", "DisplayName")]
}

df_dplyr_2 <- function(df1, df2 ,df3){
a <- inner_join(inner_join(df1, df2, by = c("WikiPostId" = "Id")), df3, by = c("OwnerUserId" = "AccountId"))
a <- select(filter(a, a$OwnerUserId != -1), c("TagName", "Count", "OwnerUserId", "Age", "Location", "DisplayName"))
a %>% arrange(desc(Count))
}

df_table_2 <- function(df1, df2, df3){
  setkey(df1, "WikiPostId")
  setkey(df2, "Id")
  setkey(df3, "AccountId")
  a <- df1[df2, nomatch = 0]
  setkey(a, "OwnerUserId")
  a <- a[df3, nomatch = 0]
  a <- a[OwnerUserId != -1, c("TagName", "Count", "OwnerUserId", "Age", "Location", "DisplayName")]
  a[order(a$Count, decreasing = TRUE)]
}

#3
df_sql_3 <- function(df1, df2){sqldf("SELECT df1.Title, RelatedTab.NumLinks FROM
(SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks
FROM df2 GROUP BY RelatedPostId) AS RelatedTab
JOIN df1 ON RelatedTab.PostId=df1.Id
WHERE df1.PostTypeId=1
ORDER BY NumLinks DESC")}

df_base_3 <- function(df1, df2){
df2[ ,c("RelatedPostId")]
RelatedTab <- aggregate(df2$RelatedPostId, df2[c("RelatedPostId")], length)
RelatedTab
colnames(RelatedTab) <- c("PostId", "NumLinks")
x <- merge(RelatedTab, df1, by.x = "PostId", by.y = "Id")
x <- x[x$PostTypeId == 1, c("Title", "NumLinks")]
x[order(x$NumLinks, decreasing = TRUE), ]
}

df_dplyr_3 <- function(df1, df2){
RelatedTab <- count(df2, df2$RelatedPostId)
RelatedTab <- RelatedTab %>% rename(c("PostId" = "df2$RelatedPostId", "NumLinks" = "n"))
x <- inner_join(RelatedTab, df1, by = c("PostId" = "Id"))
x <- select(filter(x, x$PostTypeId == 1), c("Title", "NumLinks"))
x %>% arrange(desc(NumLinks))
}

df_table_3 <- function(df1, df2){
RelatedTab <- df2[ , .N , by = .(RelatedPostId)]
setnames(RelatedTab, c("RelatedPostId", "N"), c("PostId", "NumLinks"))
setkey(df1, "Id")
setkey(RelatedTab, "PostId")
a2 <- RelatedTab[df1, nomatch = 0]
a2 <- a2[PostTypeId == 1, .(Title, NumLinks)]
setorder(a2, -NumLinks)
a2
}

#4
df_sql_4 <- function(df1, df2){
sqldf("SELECT DISTINCT
df1.Id,
df1.DisplayName,
df1.Reputation,
df1.Age,
df1.Location
FROM (
SELECT
Name, UserID
FROM df2
WHERE Name IN (
SELECT
Name
FROM df2
WHERE Class=1
GROUP BY Name
HAVING COUNT(*) BETWEEN 2 AND 10
)
AND Class=1
) AS ValuableBadges
JOIN df1 ON ValuableBadges.UserId=df1.Id")
}

df_base_4 <- function(df1, df2){
x <- df2[df2$Class == 1, ]
x <- aggregate(x$Name, x["Name"], length)
x <- x[x$x >=2 & x$x <= 10, ]
x <- x[, "Name"]
ValuableBadges <- df2[df2$Class == 1 & df2$Name %in% x, c("Name", "UserId")]
y <- merge(df1, ValuableBadges, by.x = "Id", by.y = "UserId")
unique(y[c("Id", "DisplayName", "Reputation", "Age", "Location")])
}

df_dplyr_4 <- function(df1, df2){
x <- filter(df2, df2$Class == 1)
x <- count(x, x$Name)
x <- pull(select(filter(x, x$n >= 2 & x$n <= 10), "x$Name"))
ValuableBadges <- filter(df2, df2$Class == 1 & df2$Name %in% x)
x <- inner_join(df1, ValuableBadges, by = c("Id" = "UserId"))
x <- select(x, c("Id", "DisplayName", "Reputation", "Age", "Location"))
distinct(x)
}

df_table_4 <- function(df1, df2){
y <- df2[df2$Class == 1, .N , by = .(Name)]
y <- y[y$N >= 2 & y$N <= 10, Name]
ValuableBadges <- df2[df2$Name %in% y & df2$Class == 1, .(Name, UserId)]
setkey(df1, "Id")
setkey(ValuableBadges, "UserId")
y <- df1[ValuableBadges, nomatch = 0]
unique(y[, .(Id, DisplayName, Reputation, Age, Location)])
}


#5
df_sql_5 <- function(df1){
sqldf("SELECT UpVotesTab.PostId, UpVotesTab.UpVotes, IFNULL(DownVotesTab.DownVotes, 0) AS DownVotes
FROM
(
SELECT PostId, COUNT(*) AS UpVotes FROM df1
WHERE VoteTypeId=2 GROUP BY PostId
) AS UpVotesTab
LEFT JOIN
(
SELECT PostId, COUNT(*) AS DownVotes FROM df1
WHERE VoteTypeId=3 GROUP BY PostId
) AS DownVotesTab
ON UpVotesTab.PostId=DownVotesTab.PostId")
}

df_base_5 <- function(df1){
UpVotes <- df1[df1$VoteTypeId == 2, ]
UpVotesTab <- aggregate(UpVotes$PostId, UpVotes[c("PostId")], length)
colnames(UpVotesTab) <- c("PostId", "UpVotes")
DownVotes <- df1[df1$VoteTypeId == 3, ]
DownVotesTab <- aggregate(DownVotes$PostId, DownVotes[c("PostId")], length)
Vot <- merge(UpVotesTab, DownVotesTab, by = "PostId", all.x = TRUE, all.y = FALSE)
Vot$x[is.na(Vot$x)] <- 0
colnames(Vot)<- c("PostId", "UpVotes", "DownVotes") 
Vot[, "DownVotes"] <- sapply(Vot[, "DownVotes"], as.integer) 
Vot
}

df_dplyr_5 <- function(df1){
Vot <- filter(df1,df1$VoteTypeId == 2) 
UpVotesTab <- count(Vot, Vot$PostId)
UpVotesTab <- UpVotesTab %>% rename(c("PostId" = "Vot$PostId", "UpVotes" = "n"))
Vot <- filter(df1,df1$VoteTypeId == 3)
DownVotesTab <- count(Vot, Vot$PostId)
DownVotesTab <- DownVotesTab %>% rename(c("PostId" = "Vot$PostId", "DownVotes" = "n"))
Vot <- left_join(UpVotesTab, DownVotesTab, by = c("PostId" = "PostId"))
Vot <- Vot %>% mutate(DownVotes = replace(DownVotes, which(is.na(DownVotes)), 0))
Vot[,"DownVotes"] <- lapply(Vot[,"DownVotes"], as.integer)
Vot
}

df_table_5 <- function(df1){
UpVotes <- df1[df1$VoteTypeId == 2, .N , by = .(PostId)]
setnames(UpVotes, c("PostId", "N"), c("PostId", "UpVotes"))
DownVotes <- df1[df1$VoteTypeId == 3, .N , by = .(PostId)]
setnames(DownVotes,  c("PostId", "N"), c("PostId", "DownVotes"))
setkey(UpVotes, "PostId")
setkey(DownVotes, "PostId")
UpVotes[DownVotes, on = "PostId", DownVotes := i.DownVotes ]
UpVotes[is.na(DownVotes), "DownVotes"] <- 0
UpVotes
}

df_sql_6 <- function(df1){
  sqldf("SELECT PostId, UpVotes-DownVotes AS Votes FROM (
SELECT UpVotesTab.PostId, UpVotesTab.UpVotes, IFNULL(DownVotesTab.DownVotes, 0) AS DownVotes
FROM
(
SELECT PostId, COUNT(*) AS UpVotes FROM df1
WHERE VoteTypeId=2 GROUP BY PostId
) AS UpVotesTab
LEFT JOIN
(
SELECT PostId, COUNT(*) AS DownVotes
FROM df1 WHERE VoteTypeId=3 GROUP BY PostId
) AS DownVotesTab
ON UpVotesTab.PostId=DownVotesTab.PostId
UNION
SELECT DownVotesTab.PostId, IFNULL(UpVotesTab.UpVotes, 0) AS UpVotes, DownVotesTab.DownVotes
FROM
(
SELECT PostId, COUNT(*) AS DownVotes FROM df1
WHERE VoteTypeId=3 GROUP BY PostId
) AS DownVotesTab
LEFT JOIN
(
SELECT PostId, COUNT(*) AS UpVotes FROM df1
WHERE VoteTypeId=2 GROUP BY PostId
) AS UpVotesTab
ON DownVotesTab.PostId=UpVotesTab.PostId
)")
}

df_base_6 <- function(df1){
  UpVotes <- df1[df1$VoteTypeId == 2, ]
  UpVotesTab <- aggregate(UpVotes$PostId, UpVotes[c("PostId")], length)
  colnames(UpVotesTab) <- c("PostId", "UpVotes")
  DownVotes <- df1[df1$VoteTypeId == 3, ]
  DownVotesTab <- aggregate(DownVotes$PostId, DownVotes[c("PostId")], length)
  Vot1 <- merge(UpVotesTab, DownVotesTab, by = "PostId", all.x = TRUE, all.y = FALSE)
  Vot1$x[is.na(Vot1$x)] <-  0
  colnames(Vot1)<- c("PostId", "UpVotes", "DownVotes") 
  Vot2 <- merge(DownVotesTab, UpVotesTab, by = "PostId", all.x = TRUE, all.y = FALSE)
  Vot2$UpVotes[is.na(Vot2$UpVotes)] <-  0
  colnames(Vot2)<- c("PostId", "DownVotes", "UpVotes") 
  Vot <- unique(rbind(Vot1, Vot2))
  Vot[, "Votes"] <- as.integer(Vot$UpVotes - Vot$DownVotes)
  Vot[, c("PostId", "Votes")]
}

df_dplyr_6 <- function(df1){
  Vot <- filter(df1,df1$VoteTypeId == 2) 
  UpVotesTab <- count(Vot, Vot$PostId)
  UpVotesTab <- UpVotesTab %>% rename(c("PostId" = "Vot$PostId", "UpVotes" = "n"))
  Vot <- filter(df1,df1$VoteTypeId == 3)
  DownVotesTab <- count(Vot, Vot$PostId)
  DownVotesTab <- DownVotesTab %>% rename(c("PostId" = "Vot$PostId", "DownVotes" = "n"))
  Vot1 <- left_join(UpVotesTab, DownVotesTab, by = c("PostId" = "PostId"))
  Vot1  <- Vot1 %>% mutate(DownVotes = replace(DownVotes, which(is.na(DownVotes)), 0))
  Vot2 <- left_join(DownVotesTab, UpVotesTab, by = c("PostId" = "PostId"))
  Vot2 <- Vot2 %>% mutate(UpVotes = replace(UpVotes, which(is.na(UpVotes)), 0))
  Vot <- union(Vot1, Vot2)
  Vot <- Vot %>% mutate(Votes = Vot$UpVotes - Vot$DownVotes)
  Vot[,"Votes"] <- lapply(Vot[,"Votes"], as.integer)
  select(Vot, c("PostId", "Votes"))
}

df_table_6 <- function(df1){
  UpVotes <- df1[df1$VoteTypeId == 2, .N , by = .(PostId)]
  setnames(UpVotes, c("PostId", "N"), c("PostId", "UpVotes"))
  DownVotes <- df1[df1$VoteTypeId == 3, .N , by = .(PostId)]
  setnames(DownVotes,  c("PostId", "N"), c("PostId", "DownVotes"))
  setkey(UpVotes, "PostId")
  setkey(DownVotes, "PostId")
  up <- UpVotes[DownVotes, on = "PostId", DownVotes := i.DownVotes ]
  up[is.na(DownVotes), "DownVotes"] <- 0
  down <- DownVotes[UpVotes, on = "PostId", UpVotes := i.UpVotes ]
  down[is.na(UpVotes), "UpVotes"] <- 0
  a1 <- unique(rbind(up, down))
  a1 <- a1[, Votes := UpVotes - DownVotes]
  a1[, c("PostId", "Votes")]
}


