USE [FAHESVIS]
GO

/****** Object:  Table [dbo].[Sync_To_QDrive_Log]    Script Date: 6/19/2024 12:26:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Sync_To_QDrive_Log](
	[Sync_id] [int] IDENTITY(1,1) NOT NULL,
	[FAHES_Receipt_No] [bigint] NOT NULL,
	[start_time] [datetime] NOT NULL,
	[end_time] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Sync_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

--------------------------------------------
CREATE OR ALTER FUNCTION FN_Core_GetDeviceDefectCount
(
    @request_id INT,
    @main_defect_id INT  --- 2100 breake ,2900 exh
)
RETURNS INT
AS
BEGIN
    DECLARE @count INT;

    -- Check if count exists for Evaluation Id = 3
    SELECT @count = COUNT(*)
    FROM Inspection_Results ir
    INNER JOIN Defect_Comments dc ON dc.Def_Comment_Id = ir.Defect_Comment_Id
    WHERE dc.Main_Defects_Id = @main_defect_id
    AND ir.Defect_Source = 2
    AND ir.Evalution_Id = 3
    AND ir.Request_Id = @request_id;

    IF @count > 0
        RETURN 2; -- Return 2 if count exists for Evaluation Id = 3

    -- Check if count exists for Evaluation Id = 4
    SELECT @count = COUNT(*)
    FROM Inspection_Results ir
    INNER JOIN Defect_Comments dc ON dc.Def_Comment_Id = ir.Defect_Comment_Id
    WHERE dc.Main_Defects_Id = @main_defect_id
    AND ir.Defect_Source = 2
    AND ir.Evalution_Id = 4
    AND ir.Request_Id = @request_id;

    IF @count > 0
        RETURN 1; -- Return 1 if count exists for Evaluation Id = 4

    -- Return 0 if counts for both Evaluation Ids 3 and 4 are not found
    RETURN 0;
END;


-----------------------------
USE [FAHESVIS]
GO

/****** Object:  View [dbo].[Fahes2QDriveVw]    Script Date: 6/19/2024 12:26:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE OR ALTER view [dbo].[Fahes2QDriveVw] as
WITH Milage AS (
    SELECT 
        IRC.Request_Id,
        IRC.Inspection_Req_Id,
        IRC.Code_Values
    FROM dbo.Inspection_Request_Config IRC
    WHERE IRC.Code_Operation = 28
),
BrakeReads AS (
    SELECT 
        brl.Inspection_Req_Id,
        brl.Request_Id,
       SUM(CASE WHEN Device_Output_Code IN ('50100', '50101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw1,
        SUM(CASE WHEN Device_Output_Code IN ('51100', '51101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw2,
        SUM(CASE WHEN Device_Output_Code IN ('52100', '52101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw3,
        SUM(CASE WHEN Device_Output_Code IN ('53100', '53101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw4,
        SUM(CASE WHEN Device_Output_Code IN ('54100', '54101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw5,
        SUM(CASE WHEN Device_Output_Code IN ('55100', '55101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw6,
		 SUM(CASE WHEN Device_Output_Code IN ('56100', '56101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw7,
        SUM(CASE WHEN Device_Output_Code IN ('57100', '57101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw8,
        SUM(CASE WHEN Device_Output_Code IN ('58100', '58101') THEN TRY_CAST(Reading_Value AS FLOAT) ELSE 0 END) AS aw9,
         MAX(CASE WHEN Device_Output_Code = '50250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb1r,
        MAX(CASE WHEN Device_Output_Code = '50251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb1l,
		MAX(CASE WHEN Device_Output_Code = '51250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb2r,
        MAX(CASE WHEN Device_Output_Code = '51251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb2l,
        MAX(CASE WHEN Device_Output_Code = '52250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb3r,
        MAX(CASE WHEN Device_Output_Code = '52251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb3l,
        MAX(CASE WHEN Device_Output_Code = '53250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb4r,
        MAX(CASE WHEN Device_Output_Code = '53251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb4l,
        MAX(CASE WHEN Device_Output_Code = '54250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb5r,
        MAX(CASE WHEN Device_Output_Code = '54251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb5l,
        MAX(CASE WHEN Device_Output_Code = '55250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb6r,
        MAX(CASE WHEN Device_Output_Code = '55251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb6l,
        MAX(CASE WHEN Device_Output_Code = '56250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb7r,
        MAX(CASE WHEN Device_Output_Code = '56251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb7l,
        MAX(CASE WHEN Device_Output_Code = '57250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb8r,
        MAX(CASE WHEN Device_Output_Code = '57251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb8l,
        MAX(CASE WHEN Device_Output_Code = '58250' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb9r,
        MAX(CASE WHEN Device_Output_Code = '58251' THEN TRY_CAST(Reading_Value AS FLOAT) END) AS pb9l
    FROM dbo.Brake_Reads_Log BRL
    GROUP BY brl.Inspection_Req_Id, brl.Request_Id
),
Emissions AS (
    SELECT
        IML.Request_Id,
        IML.Inspection_Req_Id,
        MAX(CASE WHEN Device_Output_Code = 39000 THEN try_cast(Reading_Value as varchar) END) AS co,
        MAX(CASE WHEN Device_Output_Code = 38000 THEN try_cast(Reading_Value as varchar) END) AS smoke_density,
        MAX(CASE WHEN Device_Output_Code = 39001 THEN try_cast(Reading_Value as varchar) END) AS HS
    FROM dbo.Exhaust_Emiss_Results_Log IML
    GROUP BY IML.Request_Id, IML.Inspection_Req_Id
),
Step1 AS (
    SELECT 
        Inspection_Step_Id AS StepId,
        Lane_Id AS LaneId,
        dbo.ConvertQatarTimeToUnix(Step_Start) AS StartTime,
        dbo.ConvertQatarTimeToUnix(Step_End) AS EndTime,
        Inspection_Req_Id,
        Request_Id,
		Inspector_Id,

        ROW_NUMBER() OVER (PARTITION BY Request_Id, Inspection_Req_Id ORDER BY Step_Start) AS StepNumber
    FROM dbo.Inspection_Steps IST
)

SELECT
 try_cast(FAHES_Receipt_No as bigint) AS [ID],
    rv.VIN_No,
    rv.Plate_No,
    rv.Plate_Type,
    rv.Color_Id,
   dbo.FN_GetQdriveCategorybyFahes(rv.Category_Id,null,null)  Category_Id ,
    isnull(rv.Owner_PID,0) Owner_PID,
    sr.Station_Id,
    ROW_NUMBER() OVER (ORDER BY FAHES_Receipt_No) AS qid,
    dbo.ConvertQatarTimeToUnix(RV.Created_Date) AS qtime,
    r.Payment_Method,
    isnull(ri.Total_Amount,0) Total_Amount,
    sr.Service_Type,
    milage.Code_Values AS milage,
    brakeReads.aw1,
    brakeReads.aw2,
    brakeReads.aw3,
    brakeReads.aw4,
    brakeReads.aw5,
    brakeReads.aw6,
	brakeReads.aw7,
    brakeReads.aw8,
    brakeReads.aw9,
    emissions.co,
    emissions.smoke_density,
    emissions.HS,
	brakeReads.pb1r,
    brakeReads.pb1l,
    brakeReads.pb2r,
    brakeReads.pb2l,
    brakeReads.pb3r,
    brakeReads.pb3l,
    brakeReads.pb4r,
    brakeReads.pb4l,
    brakeReads.pb5r,
    brakeReads.pb5l,
    brakeReads.pb6r,
    brakeReads.pb6l,
    brakeReads.pb7r,
    brakeReads.pb7l,
    brakeReads.pb8r,
    brakeReads.pb8l,
    brakeReads.pb9r,
    brakeReads.pb9l,
    MAX(CASE WHEN step1.StepNumber = 1 THEN step1.StartTime END) AS Step1_StartTime,
    MAX(CASE WHEN step1.StepNumber = 1 THEN step1.EndTime END) AS Step1_EndTime,
    MAX(CASE WHEN step1.StepNumber = 1 THEN step1.LaneId END) AS Step1_LaneId,
	MAX(CASE WHEN step1.StepNumber = 1 THEN step1.Inspector_Id END) AS Inspector1_Id,
    MAX(CASE WHEN step1.StepNumber = 2 THEN step1.StartTime END) AS Step2_StartTime,
    MAX(CASE WHEN step1.StepNumber = 2 THEN step1.EndTime END) AS Step2_EndTime,
    MAX(CASE WHEN step1.StepNumber = 2 THEN step1.LaneId END) AS Step2_LaneId,
	MAX(CASE WHEN step1.StepNumber = 2 THEN step1.Inspector_Id END) AS Inspector2_Id,
    MAX(CASE WHEN step1.StepNumber = 3 THEN step1.StartTime END) AS Step3_StartTime,
    MAX(CASE WHEN step1.StepNumber = 3 THEN step1.EndTime END) AS Step3_EndTime,
    MAX(CASE WHEN step1.StepNumber = 3 THEN step1.LaneId END) AS Step3_LaneId,
	MAX(CASE WHEN step1.StepNumber = 3 THEN step1.Inspector_Id END) AS Inspector3_Id,
		ir.Final_Result,
	ir.Status,
	rv.Created_By RVCreatedBy,
	dbo.FN_Registraion_GetNoOfReinspectionByVinNo(rv.VIN_No) as noOfInspection,
	 dbo.ConvertQatarTimeToUnix(dateadd(day,30, try_cast(RV.Created_Date as date))) AS deadLineDate,
	max(ir.Updated_Date)  Updated_Date ,
	max(ir.Request_Id)  Request_Id ,
	ir.Inspection_Service_Id,
	dbo.FN_Core_GetDeviceDefectCount(ir.Request_Id,2100) breakEval,
	dbo.FN_Core_GetDeviceDefectCount(ir.Request_Id,2900) exhEval

	--RV.VIN_No
	
FROM dbo.Inspection_Request IR
INNER JOIN dbo.Register_Vehicle RV ON RV.Request_Id = ir.Request_Id
INNER JOIN dbo.Service_Request SR ON sr.Request_Id = ir.Request_Id
LEFT JOIN Invoice_Details ID ON ID.Request_Id = sr.Request_Id
and id.Request_Id not in (8174,8176,8180,8182,8185,8203,8230,8431,8463,8515,8621,8659,8742,8746,8800,8841,8844,8858,8925)
LEFT JOIN Receipt R ON R.Invoice_Id = ID.Invoice_Id
LEFT JOIN Request_Invoice RI ON RI.Invoice_Id = ID.Invoice_Id
LEFT JOIN Milage milage ON milage.Request_Id = ir.Request_Id AND milage.Inspection_Req_Id = ir.Inspection_Req_Id
LEFT JOIN BrakeReads brakeReads ON brakeReads.Request_Id = ir.Request_Id AND brakeReads.Inspection_Req_Id = ir.Inspection_Req_Id
LEFT JOIN Emissions emissions ON emissions.Request_Id = ir.Request_Id AND emissions.Inspection_Req_Id = ir.Inspection_Req_Id
LEFT JOIN Step1 step1 ON step1.Request_Id = ir.Request_Id AND step1.Inspection_Req_Id = ir.Inspection_Req_Id
where ir.Inspection_Service_Id not in(4,5)
and FAHES_Receipt_No not in ('20240611010003',
'20240522010013')
GROUP BY
    FAHES_Receipt_No,
    rv.VIN_No,
    rv.Plate_No,
    rv.Plate_Type,
    rv.Color_Id,
   dbo.FN_GetQdriveCategorybyFahes(rv.Category_Id,null,null)   ,
    rv.Owner_PID,
    sr.Station_Id,
    dbo.ConvertQatarTimeToUnix(RV.Created_Date),
    r.Payment_Method,
    ri.Total_Amount,
    sr.Service_Type,
    milage.Code_Values,
    brakeReads.aw1,
    brakeReads.aw2,
    brakeReads.aw3,
    brakeReads.aw4,
    brakeReads.aw5,
    brakeReads.aw6,
	 brakeReads.aw7,
    brakeReads.aw8,
    brakeReads.aw9,
    emissions.co,
    emissions.smoke_density,
    emissions.HS,
	brakeReads.pb1r,
    brakeReads.pb1l,
    brakeReads.pb2r,
    brakeReads.pb2l,
    brakeReads.pb3r,
    brakeReads.pb3l,
    brakeReads.pb4r,
    brakeReads.pb4l,
    brakeReads.pb5r,
    brakeReads.pb5l,
    brakeReads.pb6r,
    brakeReads.pb6l,
    brakeReads.pb7r,
    brakeReads.pb7l,
    brakeReads.pb8r,
    brakeReads.pb8l,
    brakeReads.pb9r,
    brakeReads.pb9l,
		ir.Final_Result,
	ir.Status,
	rv.Created_By,
	dbo.FN_Registraion_GetNoOfReinspectionByVinNo(rv.VIN_No)  ,
	dbo.ConvertQatarTimeToUnix(dateadd(day,30, try_cast(RV.Created_Date as date))),
	ir.Inspection_Service_Id,
	dbo.FN_Core_GetDeviceDefectCount(ir.Request_Id,2100) ,
	dbo.FN_Core_GetDeviceDefectCount(ir.Request_Id,2900) 
	;
GO


---------------------------------------------------------------------------------
USE [FAHESVIS]
GO
/****** Object:  StoredProcedure [dbo].[SP_Migration_CopyToQDrive]    Script Date: 6/19/2024 12:27:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Create a stored procedure
ALTER PROCEDURE [dbo].[SP_Migration_CopyToQDrive]
AS
BEGIN
    BEGIN TRY
        -- Declare variables for cursor
        DECLARE @ID bigint,
		@Request_id int,
		@Service_Id int,
		@deadLine int,
		@Vin_No  varchar(max),
		@Start_Time datetime
		;
		--20240430010002
        -- Declare cursor
        DECLARE ID_Cursor CURSOR FOR
									WITH LatestLog AS (
													  SELECT 
													fahes_receipt_no, 
													MAX(end_time) AS latest_end_time 
													FROM 
													 Sync_To_QDrive_Log
													 GROUP BY 
													 fahes_receipt_no
																	)
																SELECT 
																 ID ,deadLineDate , Vin_No,Inspection_Service_Id ,request_id
																 FROM 
																	Fahes2QDriveVw v
																	LEFT JOIN 
																	LatestLog l 
																		ON 
																		  v.id = l.fahes_receipt_no
																			WHERE 
													  (v.updated_date IS NULL OR v.updated_date > l.latest_end_time)
											  OR l.latest_end_time IS NULL
											and id <> '20240512010034';
		
        -- Open the cursor
        OPEN ID_Cursor;

        -- Fetch the first row into the variable
        FETCH NEXT FROM ID_Cursor INTO @ID ,@deadLine ,@Vin_No,@Service_Id,@Request_id;

        -- Loop through the cursor
        WHILE @@FETCH_STATUS = 0
        BEGIN
	print(@ID);
  Print ('Start inserting into  [d2q_qdrive_inspection] ');
  set @Start_Time =getdate();
            -- Do something with @ID, for example, print it
      WITH UniqueSource AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY VIN_No ORDER BY id) AS rn
    FROM Fahes2QDriveVw
    WHERE id =@ID)

MERGE INTO [QDRIVE_FAHES_VIS_SYNC].dbo.[d2q_qdrive_inspection] AS target
USING (
    SELECT 
        TRY_CAST(id AS bigint) AS id, VIN_No, Plate_No, Plate_Type, Color_Id, Category_Id, Owner_PID, Station_Id, qid,
        TRY_CAST(qtime AS VARCHAR) AS qtime, ISNULL(Payment_Method, 1) AS Payment_Method, Total_Amount, noOfInspection AS inspection_type, RVCreatedBy, 
        TRY_CAST(qtime AS VARCHAR) AS inserted_on, milage, 
        TRY_CAST([aw1] AS VARCHAR) AS aw1, TRY_CAST([aw2] AS VARCHAR) AS aw2, TRY_CAST([aw3] AS VARCHAR) AS aw3, 
        TRY_CAST([aw4] AS VARCHAR) AS aw4, TRY_CAST([aw5] AS VARCHAR) AS aw5, TRY_CAST([aw6] AS VARCHAR) AS aw6, 
        TRY_CAST([aw7] AS VARCHAR) AS aw7, TRY_CAST([aw8] AS VARCHAR) AS aw8, TRY_CAST([aw9] AS VARCHAR) AS aw9, 
        TRY_CAST([co] AS VARCHAR) AS co, TRY_CAST([hs] AS VARCHAR(5)) AS hc, TRY_CAST([smoke_density] AS VARCHAR) AS smoke_density, 
        TRY_CAST([pb1r] AS VARCHAR) AS pb1r, TRY_CAST([pb1l] AS VARCHAR) AS pb1l, TRY_CAST([pb2r] AS VARCHAR) AS pb2r, 
        TRY_CAST([pb2l] AS VARCHAR) AS pb2l, TRY_CAST([pb3r] AS VARCHAR) AS pb3r, TRY_CAST([pb3l] AS VARCHAR) AS pb3l, 
        TRY_CAST([pb4r] AS VARCHAR) AS pb4r, TRY_CAST([pb4l] AS VARCHAR) AS pb4l, TRY_CAST([pb5r] AS VARCHAR) AS pb5r, 
        TRY_CAST([pb5l] AS VARCHAR) AS pb5l, TRY_CAST([pb6r] AS VARCHAR) AS pb6r, TRY_CAST([pb6l] AS VARCHAR) AS pb6l, 
        TRY_CAST([pb7r] AS VARCHAR) AS pb7r, TRY_CAST([pb7l] AS VARCHAR) AS pb7l, TRY_CAST([pb8r] AS VARCHAR) AS pb8r, 
        TRY_CAST([pb8l] AS VARCHAR) AS pb8l, TRY_CAST([pb9r] AS VARCHAR) AS pb9r, TRY_CAST([pb9l] AS VARCHAR) AS pb9l, 
        TRY_CAST(Step1_StartTime AS VARCHAR) AS test_begin_section1, TRY_CAST(Step1_EndTime AS VARCHAR) AS test_end_section1, 
        TRY_CAST(Step2_StartTime AS VARCHAR) AS test_begin_section2, TRY_CAST(Step2_EndTime AS VARCHAR) AS test_end_section2, 
        TRY_CAST(Step3_StartTime AS VARCHAR) AS test_begin_section3, TRY_CAST(Step3_EndTime AS VARCHAR) AS test_end_section3, 
        step1_laneId, ISNULL(Inspector3_Id, ISNULL(Inspector2_Id, Inspector1_Id)) AS inspector_id, 
        Final_Result, TRY_CAST(ISNULL(Step3_EndTime, ISNULL(Step2_EndTime, Step1_EndTime)) AS VARCHAR) AS inspected_on,try_cast( breakEval as varchar) as BreakEval ,try_cast( exhEval as varchar) exhEval, 0 AS is_imported
    FROM UniqueSource
    WHERE rn = 1
) AS source
ON target.id = source.id
WHEN MATCHED AND (
    target.license_plate <> source.Plate_No OR
    target.license_plate_type_id <> source.Plate_Type OR
    target.color_id <> source.Color_Id OR
    target.car_category_id <> source.Category_Id OR
    target.pid <> source.Owner_PID OR
    target.station_id <> source.Station_Id OR
    target.q_number <> source.qid OR
    target.q_time <> source.qtime OR
    target.payment_type <> source.Payment_Method OR
    target.fee <> source.Total_Amount OR
    target.inspection_type <> source.inspection_type OR
    target.inserted_by <> ISNULL(source.RVCreatedBy, 0) OR
    target.inserted_on <> source.inserted_on OR
    target.mileage <> source.milage OR
    target.aw1 <> source.aw1 OR
    target.aw2 <> source.aw2 OR
    target.aw3 <> source.aw3 OR
    target.aw4 <> source.aw4 OR
    target.aw5 <> source.aw5 OR
    target.aw6 <> source.aw6 OR
    target.aw7 <> source.aw7 OR
    target.aw8 <> source.aw8 OR
    target.aw9 <> source.aw9 OR
    target.co <> source.co OR
    target.hc <> source.hc OR
    target.smoke_density <> source.smoke_density OR
    target.pb1r <> source.pb1r OR
    target.pb1l <> source.pb1l OR
    target.pb2r <> source.pb2r OR
    target.pb2l <> source.pb2l OR
    target.pb3r <> source.pb3r OR
    target.pb3l <> source.pb3l OR
    target.pb4r <> source.pb4r OR
    target.pb4l <> source.pb4l OR
    target.pb5r <> source.pb5r OR
    target.pb5l <> source.pb5l OR
    target.pb6r <> source.pb6r OR
    target.pb6l <> source.pb6l OR
    target.pb7r <> source.pb7r OR
    target.pb7l <> source.pb7l OR
    target.pb8r <> source.pb8r OR
    target.pb8l <> source.pb8l OR
    target.pb9r <> source.pb9r OR
    target.pb9l <> source.pb9l OR
    target.test_begin_section1 <> source.test_begin_section1 OR
    target.test_end_section1 <> source.test_end_section1 OR
    target.test_begin_section2 <> source.test_begin_section2 OR
    target.test_end_section2 <> source.test_end_section2 OR
    target.test_begin_section3 <> source.test_begin_section3 OR
    target.test_end_section3 <> source.test_end_section3 OR
    target.lane_no <> source.step1_laneId OR
    target.inspector_id <> source.inspector_id OR
    target.final_eval <> source.Final_Result OR
    target.inspected_on <> source.inspected_on or
	target.eval_bs <> source.breakEval or
		target.eval_Exhaust <> source.exhEval 
) THEN
    UPDATE SET
        target.id = source.id,
        target.license_plate = source.Plate_No,
        target.license_plate_type_id = source.Plate_Type,
        target.color_id = source.Color_Id,
        target.car_category_id = source.Category_Id,
        target.pid = source.Owner_PID,
        target.station_id = source.Station_Id,
        target.q_number = source.qid,
        target.q_time = source.qtime,
        target.payment_type = source.Payment_Method,
        target.fee = source.Total_Amount,
        target.inspection_type = source.inspection_type,
        target.inserted_by = ISNULL(source.RVCreatedBy, 0),
        target.inserted_on = source.inserted_on,
        target.mileage = source.milage,
        target.aw1 = source.aw1,
        target.aw2 = source.aw2,
        target.aw3 = source.aw3,
        target.aw4 = source.aw4,
        target.aw5 = source.aw5,
        target.aw6 = source.aw6,
        target.aw7 = source.aw7,
        target.aw8 = source.aw8,
        target.aw9 = source.aw9,
        target.co = source.co,
        target.hc = source.hc,
        target.smoke_density = source.smoke_density,
        target.pb1r = source.pb1r,
        target.pb1l = source.pb1l,
        target.pb2r = source.pb2r,
        target.pb2l = source.pb2l,
        target.pb3r = source.pb3r,
        target.pb3l = source.pb3l,
        target.pb4r = source.pb4r,
        target.pb4l = source.pb4l,
        target.pb5r = source.pb5r,
        target.pb5l = source.pb5l,
        target.pb6r = source.pb6r,
        target.pb6l = source.pb6l,
        target.pb7r = source.pb7r,
        target.pb7l = source.pb7l,
        target.pb8r = source.pb8r,
        target.pb8l = source.pb8l,
        target.pb9r = source.pb9r,
        target.pb9l = source.pb9l,
        target.test_begin_section1 = source.test_begin_section1,
        target.test_end_section1 = source.test_end_section1,
        target.test_begin_section2 = source.test_begin_section2,
        target.test_end_section2 = source.test_end_section2,
        target.test_begin_section3 = source.test_begin_section3,
        target.test_end_section3 = source.test_end_section3,
        target.lane_no = source.step1_laneId,
        target.inspector_id = source.inspector_id,
        target.final_eval = source.Final_Result,
        target.inspected_on = source.inspected_on,
		target.eval_bs = source.breakEval,
		target.eval_Exhaust = source.exhEval,
        target.is_imported = source.is_imported
WHEN NOT MATCHED THEN
    INSERT ([id], [vin], [license_plate], [license_plate_type_id], [color_id], [car_category_id], [pid], [station_id], [q_number],
            [q_time], [payment_type], [fee], [inspection_type], [inserted_by], [inserted_on], [mileage], [aw1], [aw2], [aw3], 
            [aw4], [aw5], [aw6], [aw7], [aw8], [aw9], [co], [hc], [smoke_density], [pb1r], [pb1l], [pb2r], [pb2l], [pb3r], 
            [pb3l], [pb4r], [pb4l], [pb5r], [pb5l], [pb6r], [pb6l], [pb7r], [pb7l], [pb8r], [pb8l], [pb9r], [pb9l], 
            [test_begin_section1], [test_end_section1], [test_begin_section2], [test_end_section2], [test_begin_section3], 
            [test_end_section3], [lane_no], [inspector_id], [final_eval], [inspected_on], is_imported,eval_bs,eval_Exhaust)
    VALUES (
        source.id, source.VIN_No, source.Plate_No, source.Plate_Type, source.Color_Id, source.Category_Id, source.Owner_PID, source.Station_Id, source.qid,
        source.qtime, source.Payment_Method, source.Total_Amount, source.inspection_type, ISNULL(source.RVCreatedBy, 0), source.inserted_on, source.milage, 
        source.aw1, source.aw2, source.aw3, source.aw4, source.aw5, source.aw6, source.aw7, source.aw8, source.aw9, 
        source.co, source.hc, source.smoke_density, source.pb1r, source.pb1l, source.pb2r, source.pb2l, source.pb3r, 
        source.pb3l, source.pb4r, source.pb4l, source.pb5r, source.pb5l, source.pb6r, source.pb6l, source.pb7r, source.pb7l, 
        source.pb8r, source.pb8l, source.pb9r, source.pb9l, source.test_begin_section1, source.test_end_section1, source.test_begin_section2, 
        source.test_end_section2, source.test_begin_section3, source.test_end_section3, source.step1_laneId, source.inspector_id, 
        source.Final_Result, source.inspected_on, source.is_imported,source.breakEval,source.exhEval
    );

	select * from [QDRIVE_FAHES_VIS_SYNC].dbo.[d2q_qdrive_inspection]
	Print ('END inserting into  [d2q_qdrive_inspection] ');
	--select @Request_id = Request_Id ,@Service_Id = Inspection_Service_Id from Inspection_Request where try_cast(FAHES_Receipt_No as bigint) = try_cast(@ID as bigint);
	---------------------------------------
	--print('cont');

	------------------- Contact--------------------------
	Print ('Start inserting into  [[d2q_qdrive_inspection_contact_info] ');
MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_contact_info] AS Target
USING (
    SELECT @id AS id,
           Contact_Person_Phone AS mobile_number
    FROM dbo.Register_Vehicle
    WHERE Request_Id = @Request_id
) AS Source
ON Target.id = Source.id -- Assuming id is the key to match on
WHEN MATCHED AND Target.mobile_number <> Source.mobile_number THEN
    UPDATE SET
        Target.mobile_number = Source.mobile_number,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, mobile_number, is_imported)
    VALUES (Source.id, Source.mobile_number, 0);

	print ('End inserting into  [[d2q_qdrive_inspection_contact_info] ');
--------------------------------
----------------- Credit-------------

print ('Start inserting into [d2q_qdrive_inspection_credit] ');

MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_credit] AS Target
USING (
    SELECT DISTINCT @id AS id,
                    ISNULL(owner_pid, 0) AS owner_pid
    FROM dbo.Register_Vehicle RV
    INNER JOIN dbo.Invoice_Details id ON rv.Request_id = id.Request_id
    INNER JOIN Receipt PR ON PR.Invoice_Id = id.Invoice_Id AND PR.Payment_method = 4  
    WHERE rv.Request_Id = @Request_id
) AS Source
ON Target.id = Source.id -- Assuming id is the key to match on
WHEN MATCHED AND Target.pid <> Source.owner_pid THEN
    UPDATE SET
        Target.pid = Source.owner_pid,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, pid, is_imported)
    VALUES (Source.id, Source.owner_pid, 0);


	print ('End inserting into [d2q_qdrive_inspection_credit] ');
	----------------------------------------------
	
	----------------- Wight and payload-------------
	print ('Start inserting into [[d2q_qdrive_inspection_weight] ');
MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_weight] AS Target
USING (
    SELECT DISTINCT @id AS id,
                    ISNULL(rv.Weight, 0) AS weight,
                    ISNULL(rv.payload_weight, 0) AS payload_weight
    FROM dbo.Register_Vehicle RV
    WHERE rv.Request_Id = @Request_id
) AS Source
ON Target.id = Source.id -- Assuming id is the key to match on
WHEN MATCHED AND (Target.weight <> Source.weight OR Target.payload_weight <> Source.payload_weight) THEN
    UPDATE SET
        Target.weight = Source.weight,
        Target.payload_weight = Source.payload_weight,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, weight, payload_weight, is_imported)
    VALUES (Source.id, Source.weight, Source.payload_weight, 0);

	print ('End inserting into [[d2q_qdrive_inspection_weight] ');
	----------------------------------------------

	------- Join Patorl  ---
	-- Check if Service_Id is 2
		print ('Start inserting into [d2q_qdrive_inspection_joint_patrol] ');
IF @Service_Id = 2
BEGIN
    -- Print 'in' if the condition is true
    PRINT ('in');
    
    -- Use IF NOT EXISTS for a more concise check and insert statement
    IF NOT EXISTS (
        SELECT 1
        FROM [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_joint_patrol]
        WHERE id = @ID
    )
    BEGIN
        INSERT INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_joint_patrol] (id, is_imported)
        VALUES (@ID, 0);
    END;
END;
		print ('End inserting into [d2q_qdrive_inspection_joint_patrol] ');
	-----------------------------------


	----  DeadLine---------------------------
	print ('Start inserting into [d2q_qdrive_inspection_cycle_deadline] ');
	IF NOT EXISTS (
    SELECT 1
    FROM [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_cycle_deadline]
    WHERE id = @ID
)
BEGIN
    INSERT INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_cycle_deadline] (id, dead_line_on, is_imported)
    VALUES (@ID, @deadLine, 0);
END;
	print ('End inserting into [d2q_qdrive_inspection_cycle_deadline] ');

	------------------------------ Inspectecr dtl 
	
	print ('Start inserting into [d2q_qdrive_inspection_inspector_details] ');
	MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_inspector_details] AS Target
USING (
    SELECT DISTINCT  CAST(CONCAT(CAST(@id AS VARCHAR(50)), FORMAT(ROW_NUMBER() OVER (ORDER BY @id), '000')) AS BIGINT)  AS id,
                    @id AS inIdd,
                    inspector_id,
                    lane_id,
                    ROW_NUMBER() OVER (ORDER BY inspection_step_id) section_id
    FROM Inspection_Steps
    WHERE Request_Id = @Request_id
	
) AS Source
ON Target.id = Source.id -- Assuming id is the key to match on
WHEN MATCHED AND (Target.inspector_id <> Source.inspector_id OR Target.lane_id <> Source.lane_id OR Target.section_id <> Source.section_id OR Target.inspection_id <> Source.inIdd) THEN
    UPDATE SET
        Target.inspector_id = Source.inspector_id,
        Target.lane_id = Source.lane_id,
        Target.section_id = Source.section_id,
        Target.inspection_id = Source.inIdd,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, inspection_id, lane_id, section_id, inspector_id, is_imported)
    VALUES (Source.id, Source.inIdd, Source.lane_id, Source.section_id, Source.inspector_id, 0);


	print ('End inserting into [d2q_qdrive_inspection_inspector_details] ');
	------------------------------------------------
	-----------------------------vd---------------
	print ('Start inserting into [[d2q_qdrive_inspection_vd]] ');

	delete from [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_vd] where inspection_id = @Id ;

	MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_vd] AS Target
USING (
    SELECT 
        CAST(CONCAT(CAST(@id AS VARCHAR(50)), FORMAT(ROW_NUMBER() OVER (ORDER BY @id), '000')) AS BIGINT) AS Id,
        @Id AS InId,
        dc.Sub_Defect_Id AS defectId,
        REPLACE(dc.Def_Comment_Code, '/', '') AS CommentId,
        ISNULL(Remarks, 'n/A') AS Remarks,
        Inspection_Results.Location,
        CASE Evalution_Id
            WHEN 1 THEN 99
            WHEN 2 THEN 0
            WHEN 3 THEN 2
            WHEN 4 THEN 1
            ELSE 0
        END AS eval ,
		ROW_NUMBER() OVER (ORDER BY Inspection_Result_id) orderc
    FROM dbo.Inspection_Results
    INNER JOIN Defect_Comments dc ON dc.Def_Comment_Id = Defect_Comment_Id AND dc.Def_Comment_Code != '2105/24 /0'
    WHERE Request_Id = @Request_Id
	
	
) AS Source
ON Target.id = Source.id 
   
WHEN MATCHED AND (
    Target.[inspection_id] <> Source.InId OR
    Target.[defect_id] <> Source.defectId OR
    Target.[comment_id] <> Source.CommentId OR
    Target.[additional_comment] <> ISNULL(Source.Remarks, 'n/A') OR
    Target.location_id <> Source.[Location] OR
    Target.eval <> Source.eval
) THEN
    UPDATE SET
        Target.[inspection_id] = Source.InId,
        Target.[defect_id] = Source.defectId,
        Target.[comment_id] = Source.CommentId,
        Target.[additional_comment] = ISNULL(Source.Remarks, 'n/A'),
        Target.location_id = Source.[Location],
        Target.eval = Source.eval,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, [inspection_id], [defect_id], [comment_id], [additional_comment], location_id, eval, is_imported)
    VALUES (Source.id, Source.InId, Source.defectId, Source.CommentId, ISNULL(Source.Remarks, 'n/A'), Source.[Location], Source.eval, 0);


	print ('End inserting into [[d2q_qdrive_inspection_vd]] ');

	

------------------------------------------------------------------

	-----------------------------vd LOF---------------
	print ('Start inserting into [d2q_vims_inspection_visual_defect_log]'); 
	--delete from  [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_vims_inspection_visual_defect_log] where ins
	MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_vims_inspection_visual_defect_log] AS Target
USING (
    SELECT 
       CAST(CONCAT(CAST(@id AS VARCHAR(50)), FORMAT(ROW_NUMBER() OVER (ORDER BY @id), '000')) AS BIGINT) as id,
	   @id InsId,
	   REPLACE(dc.Def_Comment_Code, '/', '') AS CommentId,
	   Remarks,
	   Inspection_Results_Audit.[Location],
	    CASE Evalution_Id
            WHEN 1 THEN 99
            WHEN 2 THEN 0
            WHEN 3 THEN 2
            WHEN 4 THEN 1
            ELSE 0
        END AS eval,
		1 as lane_id, --lane
		section_id,
		1 reason, ---reason
		isnull(Inspection_Results_Audit.Created_By,0) as Created_By ,
		CASE Operation_Type
            WHEN 'D' THEN 3
            WHEN 'I' THEN 1
            WHEN 'U' THEN 2
        END AS ACTION_ID,
		[dbo].[ConvertQatarTimeToUnix](Inspection_Results_Audit.Transaction_Date) InsertedOn,
		ROW_NUMBER() OVER (ORDER BY Inspection_Results_Audit.Transaction_Date) ord
    FROM dbo.Inspection_Results_Audit
    INNER JOIN Defect_Comments dc ON dc.Def_Comment_Id = Defect_Comment_Id AND dc.Def_Comment_Code != '2105/24 /0'
   WHERE Request_Id = @Request_Id

   
) AS Source
ON  Target.id = Source.Id

WHEN MATCHED AND (
    Target.[inspection_id] <> Source.InsId OR
    Target.[comment_id] <> Source.CommentId OR
    Target.[additional_comment] <> ISNULL(Source.Remarks, 'n/A') OR
    Target.location_id <> Source.[Location] OR
    Target.eval <> Source.eval or
	target.[lane_id] = source.lane_id or
		target.stage_id = source.section_id or
		target.[added_by_type_id] = source.reason or
		target.[inspector_id] = source.Created_By or
		target.[action_id] = source.[action_id] or
		target.[inserted_on] = source.InsertedOn
) THEN
    UPDATE SET
       -- Target.[inspection_id] = Source.InsId,
       -- Target.[comment_id] = Source.CommentId,
        Target.[additional_comment] = ISNULL(Source.Remarks, 'n/A'),
        Target.location_id = Source.[Location],
        Target.eval = Source.eval,
		target.[lane_id] = source.lane_id,
		target.stage_id = source.section_id,
		target.[added_by_type_id] = source.reason,
		target.[inspector_id] = source.Created_By,
		--target.[action_id] = source.[action_id],
		target.[inserted_on] = source.InsertedOn,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, [inspection_id],  [comment_id], [additional_comment], location_id, eval, [lane_id],stage_id,[added_by_type_id],[inspector_id],[action_id],[inserted_on],is_imported)
    VALUES (Source.id, Source.InsId, Source.CommentId, ISNULL(Source.Remarks, 'n/A'),Source.[Location], Source.eval,source.lane_id,source.section_id,source.reason,source.Created_By,source.ACTION_ID,source.InsertedOn, 0);


	print ('End inserting into [[[d2q_vims_inspection_visual_defect_log]]] ');

	

------------------------------------------------------------------

-------------------------------- card ---------------
print ('Start inserting into [d2q_qdrive_inspection_card]');

MERGE INTO [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_inspection_card] AS Target
USING (
       select Attempt_Id id,
       @Id InsId,
	   id.Sub_Amount Fee,
	(Payment_Result.Created_By) Created_By,
			[dbo].[ConvertQatarTimeToUnix](Payment_Result.Created_Date) Created_Date ,
		Payment_Result.Card_No,
		0 as expires_on,
		try_cast(Auth_Code as varchar(6)) as Auth_Code,
		POS_Id,
		RRN ,
		1 tran_type,
		0 inserted_at
		from Payment_Result 
		inner join dbo.Invoice_Details ID on id.Invoice_Id = Payment_Result.Invoice_Id
		and id.Request_id =@request_id
	)as Source
	ON Target.id = Source.id -- Assuming id is the key to match on
WHEN MATCHED AND ( 
                       target.[inserted_on] <> source.Created_Date
)
THEN
    UPDATE SET
        Target.[inspection_id] = Source.InsId,
        Target.[transaction_type_id] = Source.tran_type,
        Target.[fee] = source.fee,
        Target.[inserted_by] = Source.Created_By,
        Target.[inserted_on] = Source.Created_Date,
		target.[inserted_at] = source.inserted_at,
		target.[card_number] = source.Card_No,
		target.[expires_on] = source.expires_on,
		target.[authorization_number] = source.Auth_Code,
		target.[terminal_number] = source.POS_Id,
		target.[reference_number] = source.RRN,
		target.[transaction_on] = source.Created_Date,
        Target.is_imported = 0
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, 
	[inspection_id],
	[transaction_type_id], 
	[fee], 
	[inserted_by], 
	[inserted_on], 
	[inserted_at],
	[card_number],
	[expires_on],
	[authorization_number],
	[terminal_number],
	[reference_number],
	[transaction_on],
	is_imported)
    VALUES (Source.id, 
	Source.InsId, 
	Source.tran_type, 
	source.fee,
	Source.Created_By,
	Source.Created_Date,
	source.inserted_at,
	source.Card_No,
	source.expires_on,
	source.Auth_Code,
	Source.POS_Id,
	source.RRN,
	source.Created_Date ,
	0);


print ('end inserting into [d2q_qdrive_inspection_card]');
-----------------------------------------------------------------------------------
----------------------------------- cars-------
print ('Start inserting into [dbo].d2q_qdrive_vehicle');
MERGE [QDRIVE_FAHES_VIS_SYNC].[dbo].d2q_qdrive_vehicle AS target
USING (
    SELECT 
        VIN_No AS id,
        Plate_No AS license_plate,
        Plate_Type AS license_plate_type_id,
        0 AS axle_nr,
        Manufacturer_Id AS car_manufacturer_id,
        isnull(Vehicle_Model_Id,0) AS car_model_id,
          dbo.FN_GetQdriveCategorybyFahes(Category_Id,null,null)  AS car_category_id,
        Manufacturer_Year AS year_manufact,
        Color_Id AS color_id,
        '0' AS accessories,
        Cylinders AS cylinders,
        '' AS qtic_code, -- Replace '' with appropriate value if available
        0 AS disabled,
       isnull(Created_By,0) AS inserted_by,
       [dbo].[ConvertQatarTimeToUnix](Created_Date) AS inserted_on,
       isnull(Created_By,0) AS updated_by,
       [dbo].[ConvertQatarTimeToUnix](getdate())   AS updated_on,
        0 AS is_imported
    FROM Register_Vehicle
	where Register_Vehicle.request_id=@request_id
) AS source
ON target.id = source.id
WHEN MATCHED AND (
    target.license_plate <> source.license_plate OR
    target.license_plate_type_id <> source.license_plate_type_id OR
    target.axle_nr <> source.axle_nr OR
    target.car_manufacturer_id <> source.car_manufacturer_id OR
    target.car_model_id <> source.car_model_id OR
    target.car_category_id <> source.car_category_id OR
    target.year_manufact <> source.year_manufact OR
    target.color_id <> source.color_id OR
    target.accessories <> source.accessories OR
    target.cylinders <> source.cylinders OR
    target.qtic_code <> source.qtic_code OR
    target.disabled <> source.disabled OR
    target.updated_by <> source.updated_by OR
    target.updated_on <> source.updated_on OR
    target.is_imported <> source.is_imported
) THEN 
    UPDATE SET
        target.license_plate = source.license_plate,
        target.license_plate_type_id = source.license_plate_type_id,
        target.axle_nr = source.axle_nr,
        target.car_manufacturer_id = source.car_manufacturer_id,
        target.car_model_id = source.car_model_id,
        target.car_category_id = source.car_category_id,
        target.year_manufact = source.year_manufact,
        target.color_id = source.color_id,
        target.accessories = source.accessories,
        target.cylinders = source.cylinders,
        target.qtic_code = source.qtic_code,
        target.disabled = source.disabled,
        target.updated_by = source.updated_by,
        target.updated_on = source.updated_on,
        target.is_imported = source.is_imported
WHEN NOT MATCHED THEN
    INSERT (
        id,
        license_plate,
        license_plate_type_id,
        axle_nr,
        car_manufacturer_id,
        car_model_id,
        car_category_id,
        year_manufact,
        color_id,
        accessories,
        cylinders,
        qtic_code,
        disabled,
        inserted_by,
        inserted_on,
        updated_by,
        updated_on,
        is_imported
    )
    VALUES (
        source.id,
        source.license_plate,
        source.license_plate_type_id,
        source.axle_nr,
        source.car_manufacturer_id,
        source.car_model_id,
        source.car_category_id,
        source.year_manufact,
        source.color_id,
        source.accessories,
        source.cylinders,
        source.qtic_code,
        source.disabled,
        source.inserted_by,
        source.inserted_on,
        source.updated_by,
        source.updated_on,
        source.is_imported
    );
	print ('End inserting into [dbo].d2q_qdrive_vehicle');
------------------------------------------------------

print ('Start inserting into [dbo].[d2q_qdrive_person]');
	MERGE [QDRIVE_FAHES_VIS_SYNC].[dbo].[d2q_qdrive_person] AS target
USING (
    SELECT 
        Owner_PID AS id,
        ISNULL(Owner_Name, '') AS fname,
        '' AS lname,
        0 AS language_id,
        ISNULL(Created_By, 0) AS inserted_by,
        [dbo].[ConvertQatarTimeToUnix](Created_Date) AS inserted_on,
        ISNULL(Created_By, 0) AS updated_by,
        [dbo].[ConvertQatarTimeToUnix](GETDATE()) AS updated_on,
        0 AS is_imported,
		isnull(contact_person_phone,0) phone
    FROM dbo.Register_Vehicle
	where Register_Vehicle.request_id=@request_id
	and Owner_PID is not null
) AS source
ON target.id = source.id
WHEN MATCHED AND (
    target.fname <> source.fname OR
    target.lname <> source.lname OR
    target.language_id <> source.language_id OR
    target.inserted_by <> source.inserted_by OR
    target.inserted_on <> source.inserted_on OR
    target.updated_by <> source.updated_by OR
    target.updated_on <> source.updated_on OR
    target.is_imported <> source.is_imported or
	target.phone <> source.phone
) THEN
    UPDATE SET
        target.fname = source.fname,
        target.lname = source.lname,
        target.language_id = source.language_id,
        target.inserted_by = source.inserted_by,
        --target.inserted_on = source.inserted_on, -- no need as discuss with Ateek
        target.updated_by = source.updated_by,
        target.updated_on = source.updated_on,
        target.is_imported = source.is_imported,
		target.phone = source.phone
WHEN NOT MATCHED THEN
    INSERT (
        id,
        fname,
        lname,
        language_id,
        inserted_by,
        inserted_on,
        updated_by,
        updated_on,
        is_imported,
		phone
    )
    VALUES (
        source.id,
        source.fname,
        source.lname,
        source.language_id,
        source.inserted_by,
        source.inserted_on,
        source.updated_by,
        source.updated_on,
        source.is_imported,
		source.phone
    );
	print ('End inserting into [dbo].[d2q_qdrive_person]');
----------------------------------------------------
 insert into Sync_To_QDrive_Log (FAHES_Receipt_No ,start_time,end_time)
                          values(@ID ,@Start_Time,GETDATE());


            -- Fetch the next row
            FETCH NEXT FROM ID_Cursor INTO @ID, @deadLine,@Vin_No,@Service_id,@Request_id;
        END

        -- Close and deallocate the cursor
        CLOSE ID_Cursor;
        DEALLOCATE ID_Cursor;
    END TRY
    BEGIN CATCH
        -- Error handling
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
		print @ErrorMessage;
        -- Raise custom error
        --RAISEERROR(-6, @ErrorMessage, @ErrorSeverity, @ErrorState);

        -- Check if the cursor is still open and deallocate if needed
        IF CURSOR_STATUS('global', 'ID_Cursor') >= 0
        BEGIN
            CLOSE ID_Cursor;
            DEALLOCATE ID_Cursor;
        END

        -- You can log the error or perform additional error handling as needed
    END CATCH
END

