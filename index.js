const { backup, backups, initializeApp } = require('firestore-export-import')
// const serviceAccount = require('./colivhq-dev.json')
const serviceAccount = require('./colivhq.json')
const fs = require('fs')
const uuid = require('uuid')
const moment = require('moment')
const db = require('./connection')

// Initiate Firebase App
// appName is optional, you can omit it.
initializeApp(serviceAccount)

const options = {
    docsFromEachCollection: 1, // limit number of documents when exporting
}


// Start exporting your data
backup('activities').then(async data => {
    console.log('DDDDDD',Object.keys(data.activities),'DDDDDDDD')
    let conn = await db.connection
    let colData = await getCollectionData(conn, data.activities, 'activities', false)
    let latest = { date: "", id: "" }
    colData.map((val, index) => {
        if (index === 0) {
            latest.date = val[27]
            latest.id = val[0]
        } else {
            if (latest.date < val[27]) {
                latest.date = val[27]
                latest.id = val[0]
            }
        }
        // console.log('======vvv', val[27], 'vvv======', val[0])
    })
    console.log("lllll", latest, "lllll")
    // console.log('-----', colData[colData.length - 1], '-------')
    await conn.query(`INSERT INTO activities (
                                                id,
                                                opportunityId,
                                                issueId,
                                                homeId,
                                                roomId,
                                                bedId,
                                                activityId,
                                                activityType,
                                                activityDescription,
                                                todo,
                                                completed,
                                                todoOwner,
                                                todoDate,
                                                meeting,
                                                meetingDate,
                                                meetingLocation,
                                                vendorId,
                                                vendorName,
                                                email,
                                                activities.from,
                                                activities.to,
                                                subject,
                                                body,
                                                recurring,
                                                frequency,
                                                startDate,
                                                endDate,
                                                createdDate,
                                                operatorId,
                                                createdBy,
                                                emailTrackingId,
                                                messageId
                                            ) VALUES ?`, [colData])
})


// backup('issues').then(async data => {
//     console.log('DDDDDD',data,'DDDDDDDD')
//     let conn = await db.connection
//     let colData = await getCollectionData(conn, data.issues, 'issues', false)
//     let latest = { date: "", id: "" }
//     colData.map((val, index) => {
//         if (index === 0) {
//             latest.date = val[10]
//             latest.id = val[0]
//         } else {
//             if (latest.date < val[10]) {
//                 latest.date = val[10]
//                 latest.id = val[0]
//             }
//         }
//         // console.log('======vvv', val[27], 'vvv======', val[0])
//     })
//     console.log("lllll", latest, "lllll")
//     console.log('-----', colData, '-------')
//     await conn.query(`INSERT INTO issues (
//                                             id,
//                                             type,
//                                             status,
//                                             homeId,
//                                             roomId,
//                                             bedId,
//                                             urgency,
//                                             description,
//                                             owner,
//                                             solvedDate,
//                                             createdDate,
//                                             operatorId,
//                                             createdBy
//                                             ) VALUES ?`, [colData])
// })

// backup('members').then(async data => {
//     // console.log('DDDDDD',data,'DDDDDDDD')
//     let conn = await db.connection
//     let colData = await getCollectionData(conn, data.members, 'members', false)
//     let latest = { date: "", id: "" }
//     colData.map((val, index) => {
//         if (index === 0) {
//             latest.date = val[21]
//             latest.id = val[0]
//         } else {
//             if (latest.date < val[21]) {
//                 latest.date = val[21]
//                 latest.id = val[0]
//             }
//         }
//         // console.log('======vvv', val, 'vvv======')
//     })
//     console.log("lllll", latest, "lllll")
//     await conn.query(`INSERT INTO members (
//                                                 id,
//                                                 type,
//                                                 status,
//                                                 firstName,
//                                                 lastName,
//                                                 image,
//                                                 gender,
//                                                 ageGroup,
//                                                 emailAddress,
//                                                 phone,
//                                                 whatsApp,
//                                                 company,
//                                                 jobTitle,
//                                                 standardizedJobTitle,
//                                                 source,
//                                                 sourceDetails,
//                                                 memberSince,
//                                                 dob,
//                                                 shortDOB,
//                                                 nationality,
//                                                 ownerUsername,
//                                                 createdDate,
//                                                 operatorId,
//                                                 createdBy,
//                                                 lastActivityDate
//                                             ) VALUES ?`, [colData])
// })


// backup('landlords').then(async data => {
//     // console.log('DDDDDD',data,'DDDDDDDD')
//     let conn = await db.connection
//     let colData = await getCollectionData(conn, data.landlords, 'landlords', false)
//     let latest = { date: "", id: "" }
//     colData.map((val, index) => {
//         if (index === 0) {
//             latest.date = val[12]
//             latest.id = val[0]
//         } else {
//             if (latest.date < val[12]) {
//                 latest.date = val[12]
//                 latest.id = val[0]
//             }
//         }
//         // console.log('======vvv', val, 'vvv======')
//     })
//     console.log("lllll", latest, "lllll")
//     await conn.query(`INSERT INTO landlords (
//                                                 id,
//                                                 firstName,
//                                                 lastName,
//                                                 emailAddress,
//                                                 phone,
//                                                 whatsApp,
//                                                 company,
//                                                 address,
//                                                 city,
//                                                 postalCode,
//                                                 country,
//                                                 user,
//                                                 createdDate,
//                                                 operatorId,
//                                                 createdBy,
//                                                 lastActivityDate
//                                             ) VALUES ?`, [colData])
// })

async function getCollectionData(conn, data, colName, subcol, parentId) {
    try {
        // console.log(data,'ddddd',colName,'dddd')
        let allData = []
        let memberLandData = { members: [], landlords: [] }
        await Promise.all(Object.keys(data).map(async id => {
            let dataForSql = { id }
            let memberForSql;
            let landlordForSql;
            if (colName && colName === "activities") {
                let actData = data[id]

                if (actData && actData.attachments && actData.attachments.length) {
                    let attachmentData = []
                    await Promise.all(actData.attachments.map(async val => {
                        let dat = await conn.query(`SELECT * FROM attachments WHERE filepath="${val.filepath}" AND activityId="${id}"`)
                        if (!dat || (dat && !dat.length))
                            attachmentData.push([uuid.v4(), val.filepath, val.url, id]);
                    }))
                    if (attachmentData && attachmentData.length)
                        await conn.query(`INSERT INTO attachments (id, filepath, url, activityId) VALUES ?`, [attachmentData])
                }

                if (actData && actData.subCollection) {
                    let emailTrackingId = await insertSubData(conn, actData.subCollection, id)
                    console.log('=========', emailTrackingId, '======')
                    actData.emailTrackingId = emailTrackingId && emailTrackingId[0] && emailTrackingId[0][0] ? emailTrackingId[0][0] : null

                }
                if (!actData.activityType)
                    actData.activityType = "Call"

                let dat = await conn.query(`SELECT * FROM activities WHERE id="${id}"`)
                // if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.opportunityId ? actData.opportunityId.id : null,
                        actData.issueId && actData.issueId.id ? actData.issueId.id : null,
                        actData.homeId && actData.homeId.id ? actData.homeId.id : null,
                        actData.roomId && actData.roomId.id ? actData.roomId.id : null,
                        actData.bedId && actData.bedId.id ? actData.bedId.id : null,
                        actData.activityId && actData.activityId.id ? actData.activityId.id : null,
                        actData.activityType,
                        actData.activityDescription ? actData.activityDescription.replace(/[\u0800-\uFFFF]/g, '') : actData.activityDescription,
                        actData.todo ? true : false,
                        actData.completed ? true : false,
                        actData.todoOwner && actData.todoOwner.id ? actData.todoOwner.id : null,
                        actData.todoDate ? new Date(moment(actData.todoDate._seconds * 1000)) : null,
                        actData.meeting ? true : false,
                        actData.meetingDate ? new Date(moment(actData.meetingDate._seconds * 1000)) : null,
                        actData.meetingLocation,
                        actData.vendorId && actData.vendorId.id ? actData.vendorId.id : null,
                        actData.vendorName,
                        actData.email,
                        actData.from,
                        actData.to,
                        actData.subject ? actData.subject.replace(/[\u0800-\uFFFF]/g, '') : actData.subject,
                        actData.body ? actData.body.replace(/[\u0800-\uFFFF]/g, '') : actData.body,
                        actData.recurring,
                        actData.frequency,
                        actData.startDate ? new Date(moment(actData.startDate._seconds * 1000)) : null,
                        actData.endDate ? new Date(moment(actData.endDate._seconds * 1000)) : null,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                        actData.operatorId && actData.operatorId.id ? actData.operatorId.id : null,
                        actData.createdBy && actData.createdBy.id ? actData.createdBy.id : null,
                        actData.emailTrackingId,
                        actData.messageId ? actData.messageId : null
                    ]

            } else if (colName && colName === "issues") {
                let actData = data[id]

                if (actData && actData.subCollection) {
                    await insertSubData(conn, actData.subCollection, id)
                }

                let dat = await conn.query(`SELECT * FROM issues WHERE id="${id}"`)
                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.type,
                        actData.status,
                        actData.homeId && actData.homeId.id ? actData.homeId.id : null,
                        actData.roomId && actData.roomId.id ? actData.roomId.id : null,
                        actData.bedId && actData.bedId.id ? actData.bedId.id : null,
                        actData.urgency,
                        actData.description,
                        actData.owner && actData.owner.id ? actData.owner.id : null,
                        actData.solvedDate ? new Date(moment(actData.solvedDate._seconds * 1000)) : null,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                        actData.operatorId && actData.operatorId.id ? actData.operatorId.id : null,
                        actData.createdBy && actData.createdBy.id ? actData.createdBy.id : null,
                    ]

            } else if (subcol && colName && colName === "issuePictures") {
                let actData = data[id]
                let dat = await conn.query(`SELECT * FROM pictures WHERE id="${id}"`)
                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.name,
                        actData.description,
                        actData.deleted,
                        actData.deletedDate ? new Date(moment(actData.deletedDate._seconds * 1000)) : null,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : new Date(),
                        actData.createdBy && actData.createdBy.id ? actData.createdBy.id : null,
                        actData.operatorId && actData.operatorId.id ? actData.operatorId.id : null,
                        actData.mainCity,
                        parentId,
                        actData.picLink ? actData.picLink : (actData.url ? actData.url : null),
                        actData.primary,
                        actData.filepath
                    ]
            } else if (subcol && colName && colName === "members") {
                let path = data[id].memberId.path.split("/")
                let collectionName = path[0]
                let collectionId = path[1]

                console.log('*******', collectionName, '=====', collectionId, '***********')
                if (collectionName && collectionName === "members") {
                    let dat = await conn.query(`SELECT * FROM issueActivityMembers WHERE memberId="${collectionId}" AND issueId="${parentId}"`)
                    if (!dat || (dat && !dat.length))
                        memberForSql = [
                            uuid.v4(),
                            collectionId,
                            parentId,
                        ]
                } else if (collectionName && collectionName === "landlords") {
                    let dat = await conn.query(`SELECT * FROM issueActivityLandlords WHERE landlordId="${collectionId}" AND issueId="${parentId}"`)
                    if (!dat || (dat && !dat.length))
                        landlordForSql = [
                            uuid.v4(),
                            collectionId,
                            parentId,
                        ]
                }
            } else if (subcol && colName && colName === "emailTracking") {
                let actData = data[id]
                let dat = await conn.query(`SELECT * FROM emailTracking WHERE id="${id}"`)
                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.event,
                        actData.receivedAt ? new Date(moment(actData.receivedAt._seconds * 1000)) : null,
                        actData.recipient,
                        actData.tag,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                        actData.deliveredAt ? new Date(moment(actData.deliveredAt._seconds * 1000)) : null,
                        parentId
                    ]
            } else if (colName && colName === "members") {
                let actData = data[id]

                if (actData && actData.subCollection) {
                    await insertSubData(conn, actData.subCollection, id)
                }
                let dat = await conn.query(`SELECT * FROM members WHERE id="${id}"`)
                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.type,
                        actData.status,
                        actData.firstName,
                        actData.lastName,
                        actData.image,
                        actData.gender,
                        actData.ageGroup,
                        actData.emailAddress,
                        actData.phone,
                        actData.whatsApp,
                        actData.company,
                        actData.jobTitle,
                        actData.standardizedJobTitle,
                        actData.source,
                        actData.sourceDetails ? JSON.stringify(actData.sourceDetails) : null,
                        actData.memberSince ? new Date(moment(actData.memberSince._seconds * 1000)) : null,
                        actData.dob ? new Date(moment(actData.dob._seconds * 1000)) : null,
                        actData.shortDOB,
                        actData.nationality,
                        actData.ownerUsername ? actData.ownerUsername.id : null,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                        actData.operatorId && actData.operatorId.id ? actData.operatorId.id : null,
                        actData.createdBy && actData.createdBy.id ? actData.createdBy.id : null,
                        actData.lastActivityDate ? new Date(moment(actData.lastActivityDate._seconds * 1000)) : null,
                    ]
            } else if (colName && colName === "landlords") {
                let actData = data[id]
                let dat = await conn.query(`SELECT * FROM landlords WHERE id="${id}"`)

                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.firstName,
                        actData.lastName,
                        actData.emailAddress,
                        actData.phone,
                        actData.whatsApp,
                        actData.company,
                        actData.address,
                        actData.city,
                        actData.postalCode,
                        actData.country,
                        actData.user && actData.user.id ? actData.user.id : null,
                        actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                        actData.operatorId && actData.operatorId.id ? actData.operatorId.id : null,
                        actData.createdBy && actData.createdBy.id ? actData.createdBy.id : null,
                        actData.lastActivityDate ? new Date(moment(actData.lastActivityDate._seconds * 1000)) : null,
                    ]
            } else if (subcol && colName && colName === "docVerification") {
                let actData = data[id]

                if (actData && actData.attachments && actData.attachments.length) {
                    let attachmentData = []
                    await Promise.all(actData.attachments.map(async val => {
                        let dat = await conn.query(`SELECT * FROM attachments WHERE filepath="${val.filepath}" AND docVerificationId="${id}"`)
                        if (!dat || (dat && !dat.length))
                            attachmentData.push([uuid.v4(), val.filepath, val.url, id]);
                    }))
                    if (attachmentData && attachmentData.length)
                        await conn.query(`INSERT INTO attachments (id, filepath, url, docVerificationId) VALUES ?`, [attachmentData])
                }

                let dat = await conn.query(`SELECT * FROM docVerification WHERE id="${id}"`)
                if (!dat || (dat && !dat.length))
                    dataForSql = [
                        id,
                        actData.passportCountry,
                        actData.passportExpiry ? new Date(moment(actData.passportExpiry._seconds * 1000)) : null,
                        actData.passportNumber,
                        actData.verificationBy ? actData.verificationBy.id : null,
                        actData.verificationDate ? new Date(moment(actData.verificationDate._seconds * 1000)) : null,
                        actData.visaCountry,
                        actData.visaExpiry ? new Date(moment(actData.visaExpiry._seconds * 1000)) : null,
                        actData.visaNumber,
                        parentId
                    ]
            } else {
                Object.keys(data[id]).map(async key => {
                    if (data[id][key] && data[id][key].path) {
                        dataForSql[key] = data[id][key]._path.segments[1]
                    } else if (data[id][key] && data[id][key]._seconds) {
                        dataForSql[key] = new Date(moment(data[id][key]._seconds * 1000))
                    } else if (data[id][key] && key === "subCollection") {
                        await insertSubData(conn, data[id][key], id)
                    } else {
                        dataForSql[key] = key === "attachments" ? null : data[id][key]
                    }
                })
            }
            if (dataForSql.keyword)
                delete dataForSql.keyword

            if (dataForSql && dataForSql.length)
                allData.push(dataForSql)

            if (memberForSql && memberForSql.length)
                memberLandData.members.push(memberForSql)

            if (landlordForSql && landlordForSql.length)
                memberLandData.landlords.push(landlordForSql)
        }))

        if (allData && allData.length)
            return allData;
        else
            return memberLandData;
    } catch (error) {
        console.log(error)
    }
}

async function insertSubData(conn, data, parentId) {
    try {
        // console.log("DDDDDDDDDD", data, "DDDDDDDDDD")
        return Promise.all(Object.keys(data).map(async subpath => {
            let pathArr = subpath.split('/')
            let subcollectionName = pathArr[pathArr.length - 1]
            let subcolData = await getCollectionData(conn, data[subpath], subcollectionName, true, parentId)
            console.log("sssss", subcollectionName, 'sssssss', subcolData, '---sss---')
            if (subcollectionName === "members" && subcolData && (subcolData.members || subcolData.landlords)) {
                if (subcolData.members && subcolData.members.length)
                    await conn.query(`INSERT INTO issueActivityMembers (
                                                                            id,
                                                                            memberId,
                                                                            issueId
                                                                        ) VALUES ?`, [subcolData.members])
                if (subcolData.landlords && subcolData.landlords.length)
                    await conn.query(`INSERT INTO issueActivityLandlords (
                                                                            id,
                                                                            landlordId,
                                                                            issueId
                                                                         ) VALUES ?`, [subcolData.landlords])
            }

            if (subcollectionName === "issuePictures" && subcolData && subcolData.length)
                await conn.query(`INSERT INTO pictures (
                                                            id,
                                                            name,
                                                            description,
                                                            deleted,
                                                            deletedDate,
                                                            createdDate,
                                                            createdBy,
                                                            operatorId,
                                                            mainCity,
                                                            issueId,
                                                            picLink,
                                                            primaryPic,
                                                            filepath
                                                        ) VALUES ?`, [subcolData])

            if (subcollectionName === "emailTracking" && subcolData && subcolData.length) {
                await conn.query(`INSERT INTO emailTracking (
                                                                id,
                                                                event,
                                                                receivedAt,
                                                                recipient,
                                                                tag,
                                                                createdDate,
                                                                deliveredAt,
                                                                activityId
                                                            ) VALUES ?`, [subcolData])
                return subcolData[0];
            }

            if (subcollectionName === "docVerification" && subcolData && subcolData.length) {
                await conn.query(`INSERT INTO docVerification (
                                                                id,
                                                                passportCountry,
                                                                passportExpiry,
                                                                passportNumber,
                                                                verificationBy,
                                                                verificationDate,
                                                                visaCountry,
                                                                visaExpiry,
                                                                visaNumber,
                                                                memberId
                                                            ) VALUES ?`, [subcolData])
            }
        }))
    } catch (error) {
        console.log(error)
    }
}