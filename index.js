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
    // console.log('DDDDDD',data,'DDDDDDDD')
    let conn = await db.connection
    let colData = await getCollectionData(conn, data.activities, 'activities', false)
    console.log('-----', colData, '-------')
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

async function getCollectionData(conn, data, colName, subcol, parentId) {
    try {
        // console.log("DDDDDD", data, 'DDDDD')
        let allData = await Promise.all(Object.keys(data).map(async id => {
            let dataForSql = { id }
            if (colName && colName === "activities") {
                let actData = data[id]

                if (actData && actData.attachments && actData.attachments.length) {
                    let attachmentData = actData.attachments.map(val => [uuid.v4(), val.filepath, val.url, id])
                    await conn.query(`INSERT INTO attachments (id, filepath, url, activityId) VALUES ?`, [attachmentData])
                }

                if (actData && actData.subCollection) {
                    let emailTrackingId = await insertSubData(conn, actData.subCollection, id)
                    // console.log('=========', emailTrackingId, '======')
                    actData.emailTrackingId = emailTrackingId && emailTrackingId[0] && emailTrackingId[0][0] ? emailTrackingId[0][0] : null

                }
                if (actData.subject)
                dataForSql = [
                    id,
                    actData.opportunityId ? actData.opportunityId.id : null,
                    actData.issueId && actData.issueId.id ? actData.issueId.id : null,
                    actData.homeId && actData.homeId.id ? actData.homeId.id : null,
                    actData.roomId && actData.roomId.id ? actData.roomId.id : null,
                    actData.bedId && actData.bedId.id ? actData.bedId.id : null,
                    actData.activityId && actData.activityId.id ? actData.activityId.id : null,
                    actData.activityType,
                    actData.activityDescription,
                    actData.todo ? true : false,
                    actData.completed,
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
                    actData.subject ? actData.subject.toString() : actData.subject,
                    actData.body,
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



            } else if (subcol && colName && colName === "members") {
                dataForSql = [
                    uuid.v4(),
                    id,
                    parentId,
                ]
                console.log('qqqqqq', dataForSql, 'qqqqqqq')
            } else if (subcol && colName && colName === "landlords") {
                dataForSql = [
                    uuid.v4(),
                    id,
                    parentId,
                ]
            } else if (subcol && colName && colName === "emailTracking") {
                let actData = data[id]
                dataForSql = [
                    id,
                    actData.event,
                    actData.receivedAt ? new Date(moment(actData.receivedAt._seconds * 1000)) : null,
                    actData.recipient,
                    actData.tag,
                    actData.createdDate ? new Date(moment(actData.createdDate._seconds * 1000)) : null,
                    actData.deliveredAt ? new Date(moment(actData.deliveredAt._seconds * 1000)) : null
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

            return dataForSql
        }))

        return allData;
    } catch (error) {
        console.log(error)
    }
}

async function insertSubData(conn, data, parentId) {
    try {
        return Promise.all(Object.keys(data).map(async subpath => {
            let pathArr = subpath.split('/')
            let subcollectionName = pathArr[pathArr.length - 1]
            let subcolData = await getCollectionData(conn, data[subpath], subcollectionName, true, parentId)
            // console.log("sssss", subcollectionName, 'sssssss', subcolData, '---sss---')
            if (subcollectionName === "members" && subcolData && subcolData.length)
                await conn.query(`INSERT INTO issueActivityMembers (
                                                                        id,
                                                                        memberId,
                                                                        activityId
                                                                    ) VALUES ?`, [subcolData])
            if (subcollectionName === "landlords" && subcolData && subcolData.length)
                await conn.query(`INSERT INTO issueActivityLandlords (
                                                                        id,
                                                                        landlordId,
                                                                        activityId
                                                                    ) VALUES ?`, [subcolData])
            if (subcollectionName === "emailTracking" && subcolData && subcolData.length) {
                await conn.query(`INSERT INTO emailTracking (
                                                                id,
                                                                event,
                                                                receivedAt,
                                                                recipient,
                                                                tag,
                                                                createdDate,
                                                                deliveredAt
                                                            ) VALUES ?`, [subcolData])
                return subcolData[0];
            }
        }))
    } catch (error) {
        console.log(error)
    }
}