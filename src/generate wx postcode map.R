library(nabor)

bom.locns <- read.csv('\\\\pwv-dc-fs03\\Employee\\ri072731\\daily processing scripts\\BoM downloading\\BoM weather station locations.csv')
bom.locns <- subset(bom.locns, !is.na(WMO) & BoM.station.ID %in% c('90180',	'87113',	'85279',	'89002',	'81123',	'87184',	'90184',	'90015',	'90182',	'86361',	'80128',	'90035',	'86383',	'84143',	'85301',	'90194',	'83055',	'85072',	'79099',	'88164',	'86038',	'83084',	'86266',	'86371',	'84016',	'84142',	'79103',	'90173',	'77010',	'79100',	'82139',	'79097',	'80091',	'87031',	'79028',	'84084',	'88109',	'86338',	'86282',	'76031',	'86077',	'90176',	'85280',	'85291',	'83024',	'83085',	'85296',	'84144',	'78015',	'85313',	'83090',	'84145',	'90175',	'90171',	'85099',	'79101',	'88051',	'86373',	'82039',	'86104',	'87168',	'81125',	'79105',	'77094',	'81049',	'86068',	'88162',	'82138',	'90186',	'85096',	'85151',	'81124'))
coordinates(bom.locns) <- c('Longitude', 'Latitude')
proj4string(bom.locns) <- coordinate.reference.systems$longitude.latitude
bom.locns.esta <- spTransform(bom.locns, coordinate.reference.systems$ESTA)

pc.esta <- unique(input.data$dw.extracts$cases.response.data[,c('Event.Coord.X', 'Event.Coord.Y', 'Event.Postcode')])
coordinates(pc.esta) <- c('Event.Coord.X', 'Event.Coord.Y')
pc.esta <- subset(pc.esta, !is.na(Event.Coord.X) & Event.Coord.X > 1000)
coordinates(pc.esta) <- c('Event.Coord.X', 'Event.Coord.Y')
proj4string(pc.esta) <- coordinate.reference.systems$ESTA

nearest <- knn(query = coordinates(pc.esta), data = coordinates(bom.locns.esta), k = 1)
pc.esta2 <- cbind(as.data.frame(pc.esta), as.data.frame(bom.locns)[nearest$nn.idx[,1],])
summary(pc.esta2)
pc.esta2$Event.Coord.X <- pc.esta2$Event.Coord.Y <- NULL
pc.esta2 <- ddply(aggregate(n ~ ., within(pc.esta2, n <- 1), sum),
                  .(Event.Postcode),
                  function(x) x[which.max(x$n),])
pc.esta2$n <- NULL
write.csv(file = 'c:/temp/wx.postcode.map.csv', pc.esta2, row.names = FALSE)
