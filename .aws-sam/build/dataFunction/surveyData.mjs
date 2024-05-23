import mysql from "mysql2/promise";

const dbConnection = mysql.createPool({
  host: "43.204.113.12",
  user: "vw-srv-0001",
  password: "Apple#123",
  port: 6603,
  database: "amazon_quicksight",
});
console.log("Hello");
export const lambdaHandler = async (event, context) => {
  try {
    const reports = JSON.parse(event.body).results;

    // Loop through each report
    for (const report of reports) {
      let { id, date, jsondata } = report;

      // Manpower data insertion
      const {
        techTeam,
        PwdHk,
        horticulture,
        houseKeeping,
        security,
        pwdCctvOp,
      } = jsondata;
      const teamsArray = [
        {
          id,
          teamName: "techTeam",
          manPowerActual: techTeam,
          date: date.slice(0, -1),
        },
        {
          id,
          teamName: "horticulture",
          manPowerActual: horticulture,
          date: date.slice(0, -1),
        },
        {
          id,
          teamName: "houseKeeping",
          manPowerActual: houseKeeping,
          date: date.slice(0, -1),
        },
        {
          id,
          teamName: "security",
          manPowerActual: security,
          date: date.slice(0, -1),
        },
        {
          id,
          teamName: "pwdHk",
          manPowerActual: PwdHk,
          date: date.slice(0, -1),
        },
        {
          id,
          teamName: "pwdCctvOp",
          manPowerActual: pwdCctvOp,
          date: date.slice(0, -1),
        },
      ];

      const manPowerQuery = `INSERT INTO humanResources (id, teamName, manPowerActual, date) VALUES (?, ?, ?, ?)`;
      await Promise.all(
        teamsArray.map((data) =>
          dbConnection.execute(manPowerQuery, [
            data.id,
            data.teamName,
            data.manPowerActual,
            data.date,
          ])
        )
      );

      // Electricity usage data insertion
      const {
        substationMeterE1,
        substationMeterD2,
        substationMeterD1,
        substationMeterC2,
        substationMeterC1,
        substationMeterAB1,
        substationMeterAB2,
        solarMeterCommonArea,
        solarMeterABMyntra,
        HTmeter,
      } = jsondata;
      const usageByMeters = [
        {
          id,
          meterName: "substationMeterE1",
          kvah: substationMeterE1.kvah,
          kwh: substationMeterE1.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterD2",
          kvah: substationMeterD2.kvah,
          kwh: substationMeterD2.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterD1",
          kvah: substationMeterD1.kvah,
          kwh: substationMeterD1.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterC2",
          kvah: substationMeterC2.kvah,
          kwh: substationMeterC2.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterC1",
          kvah: substationMeterC1.kvah,
          kwh: substationMeterC1.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterAB1",
          kvah: substationMeterAB1.kvah,
          kwh: substationMeterAB1.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "substationMeterAB2",
          kvah: substationMeterAB2.kvah,
          kwh: substationMeterAB2.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "solarMeterCommonArea",
          kvah: solarMeterCommonArea.kvah,
          kwh: solarMeterCommonArea.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "solarMeterABMyntra",
          kvah: solarMeterABMyntra.kvah,
          kwh: solarMeterABMyntra.kwh,
          date: date.slice(0, -1),
        },
        {
          id,
          meterName: "HTmeter",
          kvah: HTmeter.kvah,
          kwh: HTmeter.kwh,
          date: date.slice(0, -1),
        },
      ];

      // Fetch previous day's data for a given meter
      async function fetchPreviousDayData(meterName, currentDate) {
        const previousDayValuesQuery = `
        SELECT presentKvah, presentKwh
        FROM electricityUsage
        WHERE meterName = ? AND date < ?
        ORDER BY date DESC
        LIMIT 1
        `;
        const [previousDayValuesRows] = await dbConnection.execute(
          previousDayValuesQuery,
          [meterName, currentDate]
        );
        const previousDayValues = previousDayValuesRows[0] || {
          presentKvah: 0,
          presentKwh: 0,
        };
        return previousDayValues;
      }

      await Promise.all(
        usageByMeters.map(async (meterData) => {
          const previousDayValues = await fetchPreviousDayData(
            meterData.meterName,
            meterData.date
          );
          const electricityUsageQuery = `
          INSERT INTO electricityUsage (id, meterName, presentKvah, presentKwh, previousKvah, previousKwh, date)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `;
          await dbConnection.execute(electricityUsageQuery, [
            meterData.id,
            meterData.meterName,
            meterData.kvah,
            meterData.kwh,
            previousDayValues.presentKvah,
            previousDayValues.presentKwh,
            meterData.date,
          ]);
        })
      );

      // Fetch previous day's final reading for water usage
      async function fetchPreviousDayFinalReading(tankType, currentDate) {
        const previousDayFinalReadingQuery = `
        SELECT finalReading
        FROM waterUsage
        WHERE tankType = ? AND date < ?
        ORDER BY date DESC
        LIMIT 1

        `;
        const [previousDayFinalReadingRows] = await dbConnection.execute(
          previousDayFinalReadingQuery,
          [tankType, currentDate]
        );
        return previousDayFinalReadingRows[0]
          ? previousDayFinalReadingRows[0].finalReading
          : 0;
      }

      // Water Usage Processing
      const { genericTank, borewell, ansal, stpWatertank } = jsondata;
      const waterConsumptionByTankType = [
        {
          id,
          tankType: "genericTank",
          hardnessOfWater: genericTank.hardnessOfWater,
          finalReading: genericTank.meterReading,
          date: date.slice(0, -1),
        },
        {
          id,
          tankType: "borewell",
          hardnessOfWater: borewell.hardnessOfWater,
          finalReading: borewell.meterReading,
          date: date.slice(0, -1),
        },
        {
          id,
          tankType: "ansal",
          hardnessOfWater: ansal.hardnessOfWater,
          finalReading: ansal.meterReading,
          date: date.slice(0, -1),
        },
        {
          id,
          tankType: "stpWatertank",
          hardnessOfWater: stpWatertank.hardnessOfWater,
          finalReading: stpWatertank.meterReading,
          date: date.slice(0, -1),
        },
      ];

      await Promise.all(
        waterConsumptionByTankType.map(async (tankData) => {
          const previousDayFinalReading = await fetchPreviousDayFinalReading(
            tankData.tankType,
            tankData.date
          );
          const waterUsageQuery = `
          INSERT INTO waterUsage (id, tankType, hardnessOfWater, initialReading, finalReading, date)
          VALUES (?, ?, ?, ?, ?, ?)
        `;
          await dbConnection.execute(waterUsageQuery, [
            tankData.id,
            tankData.tankType,
            tankData.hardnessOfWater,
            previousDayFinalReading,
            tankData.finalReading,
            tankData.date,
          ]);
        })
      );
    }

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": true,
      },
      body: JSON.stringify({ message: "Data inserted successfully" }),
    };
  } catch (error) {
    console.error("Error:", error);
    return {
      statusCode: 500,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": true,
      },
      body: JSON.stringify({ message: error.message }),
    };
  }
};
