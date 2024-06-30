USE [FAHESVIS]
GO
/****** Object:  StoredProcedure [dbo].[SP_Migration_CopyFromQdriveSystem]    Script Date: 6/30/2024 9:10:25 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[SP_Migration_CopyFromQdriveSystem]
AS
BEGIN
    -- Begin a transaction
    BEGIN TRANSACTION;

    DECLARE @Error INT,
	@NewReqId int,
	@NewInvoiceId int,
	@NewAttempId int,
	@NewInsReqId INT,
	@NewStep1Id INT,
	@newStep2Id INT,
	@newStep3Id INT;

	DECLARE @InsertedInsResultsLogs TABLE (
    NewInspectionResultId INT,
    InsStepId INT
);



    SET @Error = 0; -- Initialize error flag


    -- Declare variables to hold QDrive data
    DECLARE @QDriveID bigint;
    DECLARE @VIN varchar(50);
    DECLARE @Plate_No varchar(50);
    DECLARE @Plate_Type int;
    DECLARE @Color_Id int;
    DECLARE @Category_Id int;
    DECLARE @PID varchar(50);
    DECLARE @StationID int;
    DECLARE @QNumber int;
    DECLARE @QTime int;
    DECLARE @Payment_Type int;
    DECLARE @Fee decimal(18,2);
    DECLARE @Inspection_Type int;
    DECLARE @InsertedBy int;
    DECLARE @InsertedOn int;
    DECLARE @Mileage int;
    DECLARE @Eval_SS varchar(50);
    DECLARE @Eval_BS varchar(50);
    DECLARE @Eval_Exhaust varchar(50);
    DECLARE @Test_Begin_Section1 int;
    DECLARE @Test_End_Section1 int;
    DECLARE @Test_Begin_Section2 int;
    DECLARE @Test_End_Section2 int;
    DECLARE @Test_Begin_Section3 int;
    DECLARE @Test_End_Section3 int;
    DECLARE @Lane_No int;
    DECLARE @Inspector_ID int;
    DECLARE @Final_Eval int;
    DECLARE @Inspected_On int
    DECLARE @Report_By int;
    DECLARE @Report_On int
    DECLARE @Cancelled_By int;
    DECLARE @Cancelled_On int
    DECLARE @Cancelled_At int
    DECLARE @Migration_Status bit;
	DECLARE @OLD_REQ_ID INT =-1;
	DECLARE @OLD_INS_REQ_ID INT =-1;
	DECLARE @P_Fullname varchar(max);
	DECLARE @phone varchar(max),
	@manufacturer_id int,
		@car_model_id int,
		@cylinders int,
		@year_manufactint int,
		@joinP bigint,
		@cWeight int,
		@Payload_Weight int,
		@serviceId int;

    -- Declare cursor to iterate over QDrive_Inspection
    DECLARE QDriveCursor CURSOR FOR
    SELECT top 3
        a.[id],
        [vin],
       a. [license_plate],
        a.[license_plate_type_id],
        a.[color_id],
       [dbo].[FN_GetFahesCategoryByQdriveCategory] (a.[car_category_id]) [car_category_id],
        [pid],
        [station_id],
        [q_number],
        [q_time],
        [payment_type],
        [fee],
        [inspection_type],
        a.[inserted_by],
        a.[inserted_on],
        [mileage],
        [eval_ss],
        [eval_bs],
        [eval_exhaust],
        [test_begin_section1],
        [test_end_section1],
        [test_begin_section2],
        [test_end_section2],
        [test_begin_section3],
        [test_end_section3],
        [lane_no],
        [inspector_id],
       case [final_eval] when 0 then 1
	   else final_eval end as final_eval ,
        [inspected_on],
        [report_by],
        [report_on],
        [cancelled_by],
        [cancelled_on],
        [cancelled_at],
        a.is_imported,
		isnull(c.fname + c.lname ,'') as fullname,
		isnull(c.phone ,0) phone ,
		rv.car_manufacturer_id,
		rv.car_model_id,
		rv.cylinders,
		rv.year_manufact,
		jp.id as jointPatrol,
		w.[weight],
		w.payload_weight
    FROM
       QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_inspection] a
	   inner join  QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_person] c on a.pid = c.id
	   inner join QDRIVE_FAHES_VIS_SYNC.[dbo].[q2d_qdrive_vehicle] RV on rv.id = a.vin
	   left join  QDRIVE_FAHES_VIS_SYNC.[dbo].q2d_qdrive_inspection_joint_patrol jp on jp.id = a.id
	   left join QDRIVE_FAHES_VIS_SYNC.[dbo].d2q_qdrive_inspection_weight w on w.id = a.id;



    OPEN QDriveCursor;
    FETCH NEXT FROM QDriveCursor INTO @QDriveID, @VIN, @Plate_No, @Plate_Type, @Color_Id, @Category_Id, @PID, @StationID, @QNumber, @QTime, @Payment_Type, @Fee, @Inspection_Type, @InsertedBy, @InsertedOn, @Mileage, @Eval_SS, @Eval_BS, @Eval_Exhaust, @Test_Begin_Section1, @Test_End_Section1, @Test_Begin_Section2, @Test_End_Section2, @Test_Begin_Section3, @Test_End_Section3, @Lane_No, @Inspector_ID, @Final_Eval, @Inspected_On, @Report_By, @Report_On, @Cancelled_By, @Cancelled_On, @Cancelled_At,@Migration_Status,@P_Fullname,@phone ,@manufacturer_id,
		@car_model_id ,
		@cylinders ,
		@year_manufactint ,
		@joinP,
		@cWeight ,
		@Payload_Weight 
 ;

    -- Loop through QDrive data
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Step 1: Insert data from QDrive_Inspection into Service_Request
        PRINT('-----   Insert Service Request  -------');
		print(@QDriveID);
		print(@car_model_id);
		print(@year_manufactint);
		print(@joinP);
		print(@cweight);
		---- check if its updatin
		SELECT @OLD_REQ_ID =Request_Id,
		@OLD_INS_REQ_ID =Inspection_Req_Id FROM Inspection_Request
		WHERE FAHES_Receipt_No =TRY_CAST(@QDriveID as nvarchar(4000));
	--	print('old req id' );
		--print( @OLD_REQ_ID);
		--print('old INS_REQ_ID' );
	--	print(@old_INS_REQ_ID);
set @serviceId=	 case when @joinP is not null then 5 else 1 end;
print (@serviceId);
	if @OLD_REQ_ID =-1 begin 
	print('inserting')

        INSERT INTO [dbo].[Service_Request] (
            Request_Date,
            Station_Id,
            Service_Type,
            Contact_Type,
            Contact_Person_Name,
            Contact_Person_Email,
            Contact_Person_Phone,
            Status,
            Registration_Source,
            Created_By,
            Created_Date
        )

        -- Data to be inserted comes from QDrive_Inspection
        VALUES (
            dbo.ConvertUnixTimeToQatarTime(@InsertedOn),
            @StationID,
            1, -- service_type
            1,
         @P_Fullname  ,
            NULL,
            @phone,
            case when @Mileage is not null then 8 else  3 end,
            2, ---source
            @InsertedBy,
             dbo.ConvertUnixTimeToQatarTime(@InsertedOn)
        );
		set @NewReqId = SCOPE_IDENTITY();
	
	 PRINT('-----  End Of Insert Service Request  -------');
	 	

		
        PRINT('-----   Insert into Register_Vehicle  -------');
        INSERT INTO [dbo].[Register_Vehicle] (
            [Request_Id],
            [Plate_No],
            [Plate_Type],
            [VIN_No],
            [Color_Id],
            [Category_Id],
            [Owner_Type],
            [Owner_PID],
            [Created_By],
            [Created_Date],
            Manufacturer_Id,
            Manufacturer_Year,
            Cylinders,
            Contact_Person_Phone,
			Weight,
			Payload_Weight
        )
        VALUES (
            @NewReqId, -- Get the last inserted identity value
            @Plate_No,
            @Plate_Type,
            @VIN,
            @Color_Id,
            @Category_Id,
            1,
            @PID,
            @InsertedBy,
               dbo.ConvertUnixTimeToQatarTime(@InsertedOn),
            @manufacturer_id,
            @year_manufactint,
            @cylinders,
            @phone,
			@cWeight,
			@Payload_Weight
        );

		 PRINT('-----  End Of Insert  Register_Vehiclet  -------');
	
		-------------------------------------------insert inspection request ----------------------------------------------------------------------------------------------
		  PRINT('-----  insert inspection reques  -------');
		INSERT INTO 
		DBO.Inspection_Request (
		Request_Id ,
		FAHES_Receipt_No,
		Inspection_Service_Id,
		Plate_No,
		Plate_Type,
		VIN_No,
		Inspection_Start,
		Inspection_End,
		Inspection_Type,
		Final_Result,
		Status,
		Remarks,
		Created_By,
		Created_Date,
		Actual_Lane_Id
		)
		values(

		@NewReqId,
		@QDriveID,
		@serviceId,
		@Plate_No,
		@Plate_Type,
		@VIN,
		 dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section1),
		 dbo.ConvertUnixTimeToQatarTime(isnull(@Test_End_Section3,isnull(@Test_Begin_Section2,@Test_Begin_Section1))),
		 1,
		 @Final_Eval,
		case when @Final_Eval is not null then 3 else 1 end,
		 null,
		 	 @Inspector_ID,
		  dbo.ConvertUnixTimeToQatarTime(@Inspected_On),
		  1
		
		)

		SET @NewInsReqId =SCOPE_IDENTITY();

		PRINT('----- end of insert inspection reques  -------');
				end; -- end of if old
	else
	begin
	PRINT('----- Update inspection reques  -------');
	set @NewReqId =@OLD_REQ_ID;
	set @NewInsReqId = @OLD_INS_REQ_ID;
		update
		DBO.Inspection_Request
		set
		Inspection_Start =dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section1),
		Inspection_End = dbo.ConvertUnixTimeToQatarTime(isnull(@Test_End_Section3,isnull(@Test_Begin_Section2,@Test_Begin_Section1))),
		Final_Result = @Final_Eval,
		Status = 	case when @Final_Eval is not null then 3 else 1 end ,
		Inspection_Service_Id =@serviceId
		where Request_Id = @NewReqId
		and Inspection_Req_Id = @NewInsReqId
	   ;

	   PRINT('-----  End Update inspection reques  -------');

	 

	-----------------------------------------------------------
	end; -- end else of old if
	  -----insert milage --
	   PRINT('----- Merge Milage  -------');
	  MERGE INTO Inspection_Request_Config AS target
USING (
    SELECT 
        @NewInsReqId AS Inspection_Req_Id,
        @NewReqId AS Request_Id,
        28 AS Code_Operation,
        @Mileage AS Code_Values,
        @Inspector_ID AS Created_By,
        dbo.ConvertUnixTimeToQatarTime(@Inspected_On) AS Created_Date
) AS source
ON (target.Inspection_Req_Id = source.Inspection_Req_Id AND target.Request_Id = source.Request_Id
and target.Code_Operation =source.Code_Operation)
WHEN MATCHED THEN
    UPDATE SET
        target.Code_Operation = source.Code_Operation,
        target.Code_Values = source.Code_Values,
        target.Created_By = source.Created_By,
        target.Created_Date = source.Created_Date
WHEN NOT MATCHED THEN
    INSERT (Inspection_Req_Id, Request_Id, Code_Operation, Code_Values, Created_By, Created_Date)
    VALUES (source.Inspection_Req_Id, source.Request_Id, source.Code_Operation, source.Code_Values, source.Created_By, source.Created_Date);
	   PRINT('----- End Merge Milage  -------');
	   -----------------------
	if not  exists ( select 1 from dbo.Invoice_Details  where Request_Id= @NewReqId) begin
	 PRINT('-----insert  Request_Invoice -------');
		
		insert into dbo.Request_Invoice 
		(
		Request_Invoice_Date,
		Total_Amount,
		Created_By,
		Created_Date,
		Total_Discount,
		Status
		)
		values(
		 dbo.ConvertUnixTimeToQatarTime(@InsertedOn),
		case when @Fee <0 then 0 else @Fee end,
		  @InsertedBy,
		   dbo.ConvertUnixTimeToQatarTime(@InsertedOn),
		  case when @Fee <0 then abs(@Fee) else 0 end,
		   3

		);

		set @NewInvoiceId = SCOPE_IDENTITY();
		 PRINT('----- End insert  Request_Invoice -------');
		
	
	 PRINT('-----  insert  dInvoice_Details -------');
		insert into dbo.Invoice_Details 
		(
		Invoice_Id,
		Request_Id,
		Sub_Amount,
		Sub_Discount,
		Created_By,
		Created_Date
		
		)
		values(
		@NewInvoiceId,
		@NewReqId,
		case when @Fee <0 then 0 else @Fee end,
		 case when @Fee <0 then abs(@Fee) else 0 end,
		 @InsertedBy,
		  dbo.ConvertUnixTimeToQatarTime(@InsertedOn)
		);
	 PRINT('----- end of insert  dInvoice_Details -------');


		--------------- insert recipt ---------------------------
		 PRINT('----- insert  recipt -------');
		insert Receipt
		( Total_Amount ,
		Payment_Method,
		Invoice_Id, 
		Created_By,
		Created_Date
		)
		values(
		case when @Fee <0 then 0 else @Fee end,
		caSe @Payment_Type
	when 1 then 1
	when 4then 2 
	when  2 then 4 EnD ,
		 @NewInvoiceId,
		
		 @InsertedBy,
		  dbo.ConvertUnixTimeToQatarTime(@InsertedOn)
		);

		 PRINT('----- end of insert  recipt -------');
			------------------------ card -------------
				 PRINT('----- insert  Payment_Attempted -------');
		insert into Payment_Attempted (Invoice_Id,POS_Id,Amount,Created_By,Created_Date)
	SELECT  @NewInvoiceId,
	1,
	case  when fee <0 then 0 else fee  end fee,
	inserted_by,
	 dbo.ConvertUnixTimeToQatarTime(inserted_at) 
	
  FROM [QDRIVE_FAHES_VIS_SYNC].[dbo].[q2d_qdrive_inspection_card]
  where id =@QDriveID;
 set @NewAttempId = SCOPE_IDENTITY();
  PRINT('----- end of insert  Payment_Attempted -------');

    PRINT('-----  insert  dbo.Payment_Result -------');
 insert into dbo.Payment_Result(Attempt_Id,Invoice_Id,POS_Id,Card_No,Auth_Code,RRN,Created_By,Created_Date)
 select @NewAttempId,@NewInvoiceId,1,card_number,authorization_number,reference_number,inserted_by, dbo.ConvertUnixTimeToQatarTime(inserted_at) 
 from [QDRIVE_FAHES_VIS_SYNC].[dbo].[q2d_qdrive_inspection_card]
  where id =@QDriveID;
  PRINT('-----  end insert  dbo.Payment_Result -------');

		end;
		------------------------------------------------------------------------------------------------------------------------


		--------------------------------------- inspection steps --------------------------------
				PRINT('-----  insert  inspection steps -------');
		 declare @recCount int;
	  select @recCount = count(*) from [dbo].[Inspection_Steps] where Inspection_Req_Id =@OLD_INS_REQ_ID
		and Request_Id =@OLD_REQ_ID;
		--- step one
		
		if (@Test_Begin_Section1 is not null  and @Test_Begin_Section1 <>0) begin 
		if (@recCount<1) begin
	PRINT('-----  insert  inspection steps  1 -------');
		insert [dbo].[Inspection_Steps]( 
      [Inspection_Req_Id]
      ,[Request_Id]
      ,[Inspection_Service_Id]
      ,[Lane_Id]
      ,[Section_Id]
      ,[Step_Start]
      ,[Step_End]
      ,[Inspector_Id]
      ,[Remarks])
	  values(@NewInsReqId,
	  @NewReqId,
	  @Inspection_Type,
	   1,-- to check lane
	  1,-- to check  section  should be based on the sq. 
	  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section1),
	  dbo.ConvertUnixTimeToQatarTime(@Test_End_Section1),
	  @Inspector_ID,
	  null --- Remarks to be checked
	  ); 

	 set @NewStep1Id =SCOPE_IDENTITY();
	 		PRINT('-----end of  insert  inspection steps  1 -------');
	  end;
	  else
	  begin
	 	PRINT('-----Update inspection steps  1 -------');
	  declare @Inspection_Step1_Id int;

	  select top 1 @Inspection_Step1_Id =Inspection_Step_Id from [dbo].[Inspection_Steps] where Inspection_Req_Id =@OLD_INS_REQ_ID
		and Request_Id =@OLD_REQ_ID;
		  print (@Inspection_Step1_Id);
		set  @NewStep1Id = @Inspection_Step1_Id
	  update
	  [dbo].[Inspection_Steps]
	  set
	  [Step_Start] =  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section1),
      [Step_End] = dbo.ConvertUnixTimeToQatarTime(@Test_End_Section1),
      [Inspector_Id] =@Inspector_ID,
      [Remarks] =null
	    where  [dbo].[Inspection_Steps].Request_Id =@NewReqId
	  and  [dbo].[Inspection_Steps].Inspection_Req_Id =@NewInsReqId
	  and [dbo].[Inspection_Steps].Inspection_Step_Id = @Inspection_Step1_Id
	   	PRINT('----- end Update inspection steps  1 -------');
	  end ; -- exists
	  end; --not nllexists
	  


	  ----- step 2 ---------------------
	  if  (@Test_Begin_Section2 is not null  and @Test_Begin_Section2 <>0) begin 
	 
	  if (@recCount <2 )begin
	   	PRINT('-----insert inspection steps  2 -------');
		insert [dbo].[Inspection_Steps]( 
      [Inspection_Req_Id]
      ,[Request_Id]
      ,[Inspection_Service_Id]
      ,[Lane_Id]
      ,[Section_Id]
      ,[Step_Start]
      ,[Step_End]
      ,[Inspector_Id]
      ,[Remarks])
	  values(@NewInsReqId,
	  @NewReqId,
	  @Inspection_Type,
	   1,-- to check lane
	  1,-- to check  section
	  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section2),
	  dbo.ConvertUnixTimeToQatarTime(@Test_End_Section2),
	  @Inspector_ID,
	  null --- Remarks to be checked
	  ); 
	   set @newStep2Id =SCOPE_IDENTITY();
	     	PRINT('-----end of insert inspection steps  2 -------');
	  end; -- check if exists
	  else
	  begin
	    	PRINT('-----update inspection steps  2 -------');
	   declare @Inspection_Step2_Id int;
	   WITH CTE AS (
        SELECT 
            Inspection_Step_Id,
            ROW_NUMBER() OVER (ORDER BY Inspection_Step_Id ASC) AS RowNum
        FROM 
            [dbo].[Inspection_Steps]
        WHERE 
            Inspection_Req_Id = @OLD_INS_REQ_ID
            AND Request_Id = @OLD_REQ_ID
    )
    SELECT 
        @Inspection_Step2_Id = Inspection_Step_Id
    FROM 
        CTE
    WHERE 
        RowNum = 2;
		print (@Inspection_Step2_Id);
		set  @NewStep2Id = @Inspection_Step2_Id
	  update
	  [dbo].[Inspection_Steps]
	  set
	  [Step_Start] =  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section2),
      [Step_End] = dbo.ConvertUnixTimeToQatarTime(@Test_End_Section2),
      [Inspector_Id] =@Inspector_ID,
      [Remarks] =null
	  	  where  [dbo].[Inspection_Steps].Request_Id =@NewReqId
	  and  [dbo].[Inspection_Steps].Inspection_Req_Id =@NewInsReqId
	  and [dbo].[Inspection_Steps].Inspection_Step_Id = @Inspection_Step2_Id
	  	PRINT('----- end of update inspection steps  2 -------');
	  end;
	  end;
	  	
		-------------------------------------------------
		  ----- step 3 ---------------------
	  if  (@Test_Begin_Section3 is not null  and @Test_Begin_Section3 <>0) begin 
	  if (@recCount <3 )begin
	  	PRINT('----- insert inspection steps  3 -------');
		insert [dbo].[Inspection_Steps]( 
      [Inspection_Req_Id]
      ,[Request_Id]
      ,[Inspection_Service_Id]
      ,[Lane_Id]
      ,[Section_Id]
      ,[Step_Start]
      ,[Step_End]
      ,[Inspector_Id]
      ,[Remarks])
	  values(@NewInsReqId,
	  @NewReqId,
	  @Inspection_Type,
	  1,-- to check lane
	 1,-- to check  section
	  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section3),
	  dbo.ConvertUnixTimeToQatarTime(@Test_End_Section3),
	  @Inspector_ID,
	  null --- Remarks to be checked
	  ); 
	  set @newStep3Id =SCOPE_IDENTITY();
	    	PRINT('----- end insert inspection steps  3 -------');
	  end;
	  else
	  begin
	   	PRINT('----- update inspection steps  3 -------');
	  declare @Inspection_Step3_Id int;
	   WITH CTE AS (
        SELECT 
            Inspection_Step_Id,
            ROW_NUMBER() OVER (ORDER BY Inspection_Step_Id ASC) AS RowNum
        FROM 
            [dbo].[Inspection_Steps]
        WHERE 
            Inspection_Req_Id = @OLD_INS_REQ_ID
            AND Request_Id = @OLD_REQ_ID
    )
    SELECT 
        @Inspection_Step3_Id = Inspection_Step_Id
    FROM 
        CTE
    WHERE 
        RowNum = 3;
		print (@Inspection_Step3_Id);

		set  @NewStep3Id = @Inspection_Step3_Id
	  update
	  [dbo].[Inspection_Steps]
	  set
	  [Step_Start] =  dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section3),
      [Step_End] = dbo.ConvertUnixTimeToQatarTime(@Test_End_Section3),
      [Inspector_Id] =@Inspector_ID,
      [Remarks] =null
	  where  [dbo].[Inspection_Steps].Request_Id =@NewReqId
	  and  [dbo].[Inspection_Steps].Inspection_Req_Id =@NewInsReqId
	  and [dbo].[Inspection_Steps].Inspection_Step_Id = @Inspection_Step3_Id
	   	PRINT('----- end of update inspection steps  3 -------');
	  end;
	  end;
		-------------------------------------------------

		---------------------- insert defect comment -------------
	
	 	PRINT('----- Merge Defect Comment -------');
	
		MERGE INTO dbo.Inspection_Results AS target
USING (
    SELECT 
        @NewStep1Id AS Inspection_Step_Id,
        @NewInsReqId AS Inspection_Req_Id,
        @NewReqId AS Request_Id,
        @serviceId AS Inspection_Service_Id,
        1 AS Section_Id, -- section to check
        df.Mode AS Defect_Mode, -- Defect Mode
        df.Def_Comment_Id,
        [dbo].[FN_Inspection_GetDefectClassification](df.Main_Defects_Id, df.Def_Comment_Id) AS Defect_Classification, -- classification
        qiv.additional_comment AS Remarks,
        1 AS Status,
        CASE WHEN df.Device_Comment = 0 THEN 1 ELSE 1 END AS Defect_Source,
        qiv.eval AS Evalution_Id,
        qiv.location_id AS Location,
        0 AS Axle, -- Axle not used
        @Inspector_ID AS Created_By, -- to be checked
        dbo.ConvertUnixTimeToQatarTime(@Test_Begin_Section1) AS Created_Date
    FROM 
        QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection_vd QIV
    INNER JOIN 
        dbo.Defect_Comments DF 
    ON 
        qiv.comment_id = REPLACE(df.Def_Comment_Code, '/', '')
    WHERE 
        qiv.inspection_id = @QDriveID
) AS source
ON target.Defect_Comment_Id = source.Def_Comment_Id
   AND target.Inspection_Req_Id = source.Inspection_Req_Id
   AND target.Request_Id = source.Request_Id
WHEN MATCHED THEN
    UPDATE SET 
       -- target.Inspection_Step_Id = source.Inspection_Step_Id,
       -- target.Inspection_Service_Id = source.Inspection_Service_Id,
        target.Section_Id = source.Section_Id,
        target.Defect_Mode = source.Defect_Mode,
        target.Defect_Classification = source.Defect_Classification,
        target.Remarks = source.Remarks,
        target.Status = source.Status,
        target.Defect_Source = source.Defect_Source,
        target.Evalution_Id = source.Evalution_Id,
        target.Location = source.Location,
        target.Axle = source.Axle,
        target.Created_By = source.Created_By,
        target.Created_Date = source.Created_Date
WHEN NOT MATCHED THEN
    INSERT (
        [Inspection_Step_Id],
        [Inspection_Req_Id],
        [Request_Id],
        [Inspection_Service_Id],
        [Section_Id],
        [Defect_Mode],
        [Defect_Comment_Id],
        [Defect_Classification],
        [Remarks],
        [Status],
        [Defect_Source],
        [Evalution_Id],
        [Location],
        [Axle],
        [Created_By],
        [Created_Date]
    )
    VALUES (
        source.Inspection_Step_Id,
        source.Inspection_Req_Id,
        source.Request_Id,
        source.Inspection_Service_Id,
        source.Section_Id,
        source.Defect_Mode,
        source.Def_Comment_Id,
        source.Defect_Classification,
        source.Remarks,
        source.Status,
        source.Defect_Source,
        source.Evalution_Id,
        source.Location,
        source.Axle,
        source.Created_By,
        source.Created_Date
    )
OUTPUT inserted.Inspection_Result_Id, inserted.Inspection_Step_Id INTO @InsertedInsResultsLogs;
	PRINT('----- end  Merge Defect Comment -------');

		

		--------------------- insert the device readings ---------------------------
		--breaks


	PRINT('-----   Merge Brake_Reads_Log aw -------');
	
	MERGE INTO Brake_Reads_Log AS target
USING (
    SELECT 
        @NewReqId AS Request_Id,
        @NewInsReqId AS Inspection_Req_Id,
        CASE ColumnName
            WHEN 'aw1' THEN 50100
            WHEN 'aw2' THEN 51100
            WHEN 'aw3' THEN 52100
            WHEN 'aw4' THEN 53100
            WHEN 'aw5' THEN 54100
            WHEN 'aw6' THEN 55100
            WHEN 'aw7' THEN 56100
            WHEN 'aw8' THEN 57100
            WHEN 'aw9' THEN 58100
           
        END AS Device_Output_Code,
      cast(ColumnValue as varchar(6)) AS Reading_Value
    FROM 
        QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_inspection]
    UNPIVOT (
        ColumnValue FOR ColumnName IN (aw1, aw2, aw3, aw4, aw5, aw6, aw7, aw8, aw9)
    ) AS Unpvt
    WHERE id = @QDriveID
) AS source
ON target.Request_Id = source.Request_Id 
   AND target.Inspection_Req_Id = source.Inspection_Req_Id
   AND target.Device_Output_Code = source.Device_Output_Code
WHEN MATCHED THEN 
    UPDATE SET 
        target.Reading_Value = source.Reading_Value
WHEN NOT MATCHED THEN 
    INSERT (Request_Id, Inspection_Req_Id, Device_Output_Code, Reading_Value)
    VALUES (source.Request_Id, source.Inspection_Req_Id, source.Device_Output_Code, source.Reading_Value);

		PRINT('-----  end Merge Brake_Reads_Log aw -------');
			PRINT('-----  Merge Brake_Reads_Log pb -------');
	MERGE INTO Brake_Reads_Log AS target
USING (
    SELECT 
        @NewReqId AS Request_Id,
        @NewInsReqId AS Inspection_Req_Id,
        CASE ColumnName
           
            WHEN 'pb1r' THEN 50250
            WHEN 'pb1L' THEN 50251
            WHEN 'pb2r' THEN 51250
            WHEN 'pb2L' THEN 51251
            WHEN 'pb3r' THEN 52250
            WHEN 'pb3L' THEN 52251
            WHEN 'pb4r' THEN 53250
            WHEN 'pb4L' THEN 53251
            WHEN 'pb5r' THEN 54250
            WHEN 'pb5L' THEN 54251
            WHEN 'pb6r' THEN 55250
            WHEN 'pb6L' THEN 55251
            WHEN 'pb7r' THEN 56250
            WHEN 'pb7L' THEN 56251
            WHEN 'pb8r' THEN 57250
            WHEN 'pb8L' THEN 57251
            WHEN 'pb9r' THEN 58250
            WHEN 'pb9L' THEN 58251
        END AS Device_Output_Code,
      cast(ColumnValue as varchar(6)) AS Reading_Value
    FROM 
        QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_inspection]
    UNPIVOT (
        ColumnValue FOR ColumnName IN (pb1r, pb1L, pb2r, pb2L, pb3r, pb3L, pb4r, pb4L, pb5r, pb5L, pb6r, pb6L, pb7r, pb7L, pb8r, pb8L, pb9r, pb9L)
    ) AS Unpvt
    WHERE id = @QDriveID
) AS source
ON target.Request_Id = source.Request_Id 
   AND target.Inspection_Req_Id = source.Inspection_Req_Id
   AND target.Device_Output_Code = source.Device_Output_Code
WHEN MATCHED THEN 
    UPDATE SET 
        target.Reading_Value = source.Reading_Value
WHEN NOT MATCHED THEN 
    INSERT (Request_Id, Inspection_Req_Id, Device_Output_Code, Reading_Value)
    VALUES (source.Request_Id, source.Inspection_Req_Id, source.Device_Output_Code, source.Reading_Value);

PRINT('----- end  Merge Brake_Reads_Log pb -------');
		------------------------------------------------------------------------------
		----------how to calculate  the break results


		----- exhust------------
		PRINT('-----  Merge BExhaust_Emiss_Results_Log -------');
	MERGE INTO Exhaust_Emiss_Results_Log AS target
USING (
    SELECT 
        @NewReqId AS Request_Id,
        @NewInsReqId AS Inspection_Req_Id,
        CASE ColumnName
            WHEN 'co' THEN 39000
            WHEN 'smoke_density' THEN 38000
            WHEN 'hc' THEN 39001
        END AS Device_Output_Code,
        try_cast(ColumnValue as float) AS Reading_Value
    FROM 
        QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_inspection]
    UNPIVOT (
        ColumnValue FOR ColumnName IN (co, smoke_density, hc)
    ) AS Unpvt
    WHERE id = @QDriveID
) AS source
ON target.Request_Id = source.Request_Id 
   AND target.Inspection_Req_Id = source.Inspection_Req_Id
   AND target.Device_Output_Code = source.Device_Output_Code
WHEN MATCHED THEN 
    UPDATE SET 
        target.Reading_Value = source.Reading_Value
WHEN NOT MATCHED THEN 
    INSERT (Request_Id, Inspection_Req_Id, Device_Output_Code, Reading_Value)
    VALUES (source.Request_Id, source.Inspection_Req_Id, source.Device_Output_Code, source.Reading_Value);
	PRINT('-----  end BExhaust_Emiss_Results_Log -------');
-----------------------------------------
	----------------------- service breake  --------
	PRINT('-----  Merge Brake_Results_Log -------');
	MERGE INTO Brake_Results_Log AS target
USING (
    SELECT 
        @NewReqId AS Request_Id,
        @NewInsReqId AS Inspection_Req_Id,
        Rtype AS Brake_Read_Type,
        Description_En,
        Left_Value,
        Right_Value,
		MinDec,
		maxDif,
		 ActDc,
        actDif,
		wight
    FROM Migration_Service_breake
    WHERE id = @QDriveID
	and try_cast(Left_Value as decimal(18,3)) is not null
	and try_cast(right_Value as decimal(18,3)) is not null
) AS source
ON (target.Request_Id = source.Request_Id AND target.Inspection_Req_Id = source.Inspection_Req_Id
and target.Brake_Read_Type =source.Brake_Read_Type)
WHEN MATCHED THEN
    UPDATE SET
        target.Brake_Read_Type = source.Brake_Read_Type,
        target.Description_En = source.Description_En,
        target.Left_Value = try_cast(source.Left_Value as decimal(18,3)),
        target.Right_Value = try_cast(source.right_value as decimal(18,3)),
		target.min_deceleration = source.MinDec,
		target.max_difference = source.maxDif,
		target.actual_Difference = source.actDif,
		target.actual_Deceleration = source.ActDc,
		target.weight = source.wight
WHEN NOT MATCHED THEN
    INSERT (Request_Id, Inspection_Req_Id, Brake_Read_Type, Description_En, Left_Value, Right_Value,min_deceleration,max_difference,actual_Difference,actual_Deceleration,weight)
    VALUES (source.Request_Id, source.Inspection_Req_Id, source.Brake_Read_Type, source.Description_En,try_cast(source.Left_Value as decimal(18,3)), try_cast(source.right_value as decimal(18,3)),source.MinDec,source.maxDif,source.maxDif,source.actDif,source.wight);

		PRINT('----- end Merge Brake_Results_Log -------');
			PRINT('-----  Merge Brake_Results_Log final -------');
	 MERGE INTO Brake_Results_Log AS target
USING (
    SELECT 
        Request_Id, 
        Inspection_Req_Id, 
        10 AS Brake_Read_Type,
        'Final Dec. - Service Brake' AS Description_En,
        40 AS min_deceleration,
        ROUND((SUM(Left_Value) + SUM(Right_Value)) / (SUM(weight) * 0.0000981), 0) AS actual_Deceleration
    FROM dbo.Brake_Results_Log
    WHERE Request_Id = @NewReqId
      AND Brake_Read_Type < 10
    GROUP BY Request_Id, Inspection_Req_Id
) AS source
ON (target.Request_Id = source.Request_Id 
    AND target.Inspection_Req_Id = source.Inspection_Req_Id 
    AND target.Brake_Read_Type = source.Brake_Read_Type)
WHEN MATCHED THEN
    UPDATE SET
        target.Description_En = source.Description_En,
        target.min_deceleration = source.min_deceleration,
        target.actual_Deceleration = source.actual_Deceleration
WHEN NOT MATCHED THEN
    INSERT (Request_Id, Inspection_Req_Id, Brake_Read_Type, Description_En, min_deceleration, actual_Deceleration)
    VALUES (source.Request_Id, source.Inspection_Req_Id, source.Brake_Read_Type, source.Description_En, source.min_deceleration, source.actual_Deceleration);

	PRINT('----- end  Merge Brake_Results_Log final -------');


	-------------------------------------------------------- Inspection_Results_Audit----------------------- 
	PRINT('-----  Merge Inspection_Results_Audit  -------');
	MERGE INTO dbo.Inspection_Results_Audit AS target
USING (
    SELECT 
        CASE stage_id 
            WHEN 1 THEN @NewStep1Id
            WHEN 2 THEN @newStep2Id
            WHEN 3 THEN @newStep3Id
        END AS stepId,
        @NewInsReqId AS NewInsReqId,
        @NewReqId AS NewReqId,
        @serviceId AS serviceId,
        1 AS section,
        df.Mode AS Defect_Mode, -- Defect Mode
        df.Def_Comment_Id,
        [dbo].[FN_Inspection_GetDefectClassification](df.Main_Defects_Id, df.Def_Comment_Id) AS Defect_Classification, -- classification
        qivm.additional_comment AS Remarks,
        1 AS Status,
        CASE 
            WHEN df.Device_Comment = 0 THEN 1 
            ELSE 1 
        END AS Defect_Source,
        CASE qivm.eval 
            WHEN 99 THEN 1
            WHEN 0 THEN 2
            WHEN 2 THEN 3
            WHEN 1 THEN 4
            ELSE 0
        END AS Evaluation_Id,
        qivm.location_id AS Location,
        0 AS Axle, -- Axle not used
        @Inspector_ID AS Created_By, -- to be checked
        dbo.ConvertUnixTimeToQatarTime(qivm.inserted_on) AS Created_Date,
        CASE ACTION_ID
            WHEN 3 THEN 'D'
            WHEN 1 THEN 'I'
            WHEN 2 THEN 'U'
        END AS Operation_type
    FROM 
        QDRIVE_FAHES_VIS_SYNC.[dbo].[q2d_vims_inspection_visual_defect_log] qivm
    INNER JOIN 
        dbo.Defect_Comments df 
    ON 
        qivm.comment_id = REPLACE(df.Def_Comment_Code, '/', '')
    WHERE 
        qivm.inspection_id = @QDriveID
) AS source
ON (
    target.Inspection_Step_Id = source.stepId
    AND target.Inspection_Req_Id = source.NewInsReqId
    AND target.Request_Id = source.NewReqId
    AND target.Inspection_Service_Id = source.serviceId
)
WHEN MATCHED THEN
    UPDATE SET
        target.Section_Id = source.section,
        target.Defect_Mode = source.Defect_Mode,
        target.Defect_Comment_Id = source.Def_Comment_Id,
        target.Defect_Classification = source.Defect_Classification,
        target.Remarks = source.Remarks,
        target.Status = source.Status,
        target.Defect_Source = source.Defect_Source,
        target.Evalution_Id = source.Evaluation_Id,
        target.Location = source.Location,
        target.Axle = source.Axle,
        target.Created_By = source.Created_By,
        target.Operation_Type = source.Operation_type
WHEN NOT MATCHED THEN
    INSERT (
        Inspection_Step_Id,
        Inspection_Req_Id,
        Request_Id,
        Inspection_Service_Id,
        Section_Id,
        Defect_Mode,
        Defect_Comment_Id,
        Defect_Classification,
        Remarks,
        Status,
        Defect_Source,
        Evalution_Id,
        Location,
        Axle,
        Created_By,
        Operation_Type
    )
    VALUES (
        source.stepId,
        source.NewInsReqId,
        source.NewReqId,
        source.serviceId,
        source.section,
        source.Defect_Mode,
        source.Def_Comment_Id,
        source.Defect_Classification,
        source.Remarks,
        source.Status,
        source.Defect_Source,
        source.Evaluation_Id,
        source.Location,
        source.Axle,
        source.Created_By,
        source.Operation_type
    );
		PRINT('----- end Merge Inspection_Results_Audit  -------');
	--------------------------------------------------


		UPDATE QDRIVE_FAHES_VIS_SYNC.dbo.[q2d_qdrive_inspection]
        SET is_imported = '1'-- Adjust status value as needed
        WHERE [id] = @QDriveID;
        
		
		FETCH NEXT FROM QDriveCursor INTO @QDriveID, @VIN, @Plate_No, @Plate_Type, @Color_Id, @Category_Id, @PID, @StationID, @QNumber, @QTime, @Payment_Type, @Fee, @Inspection_Type, @InsertedBy, @InsertedOn, @Mileage, @Eval_SS, @Eval_BS, @Eval_Exhaust, @Test_Begin_Section1, @Test_End_Section1, @Test_Begin_Section2, @Test_End_Section2, @Test_Begin_Section3, @Test_End_Section3, @Lane_No, @Inspector_ID, @Final_Eval, @Inspected_On, @Report_By, @Report_On, @Cancelled_By, @Cancelled_On, @Cancelled_At, @Migration_Status ,@P_fullname,@phone,@manufacturer_id,
		@car_model_id ,
		@cylinders ,
		@year_manufactint ,
		@joinP,
		@cWeight ,
		@Payload_Weight ;
		
	end; ---end of while
    END;

    CLOSE QDriveCursor;
    DEALLOCATE QDriveCursor;

    -- Check for errors
    IF @@ERROR <> 0
    BEGIN
        SET @Error = 1;
		  CLOSE QDriveCursor;
    DEALLOCATE QDriveCursor;
    END

    IF @Error = 0
    BEGIN
        -- Commit transaction
        COMMIT TRANSACTION;
			--  CLOSE QDriveCursor;
   --DEALLOCATE QDriveCursor;
        -- Return 0 to indicate success
        RETURN 0;
    END
    ELSE
    BEGIN
	  CLOSE QDriveCursor;
    DEALLOCATE QDriveCursor;
        -- Roll back the transaction in case of error
        ROLLBACK TRANSACTION;
        RETURN 1; -- Return 1 to indicate failure
    
END;


-------------------------------------
USE [FAHESVIS]
GO

/****** Object:  UserDefinedFunction [dbo].[FN_GetFahesCategoryByQdriveCategory]    Script Date: 6/30/2024 9:12:42 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create FUNCTION [dbo].[FN_GetFahesCategoryByQdriveCategory]
(
	 @QdriveCategoryId INT
)
RETURNS INT
AS
BEGIN
	DECLARE @FahesId INT;
 SELECT DISTINCT @FahesId = Fahes_Id FROM Temp_Category_Mapper
 WHERE Qdrive_Category_Id = @QdriveCategoryId;

	
	

	RETURN @FahesId

END
GO


---------------------------------------------------

USE [FAHESVIS]
GO

/****** Object:  View [dbo].[Migration_Service_breake]    Script Date: 6/30/2024 9:13:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





--INSERT INTO dbo.Brake_Results_Log (Description_En, Left_Value, Right_Value)
ALTER view [dbo].[Migration_Service_breake] as
SELECT 
    'Service Break - Axle 1' AS Description_En, 
    bffal AS Left_Value, 
    bffar AS Right_Value,
	1 as Rtype,
	id,
	45 MinDec,
	35 maxDif,
	aw1 wight,
dbo.Core_CalculateBreakActDec(bffal,bffar,aw1) as ActDc,
dbo.[Core_CalculateBreakActDif](bffal,bffar) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
UNION ALL
SELECT 
    'Service Break - Axle 2' AS Description_En, 
    bfral AS Left_Value, 
    bfrar AS Right_Value,
	2 as Rtype,
	id,
	35 MinDec,
	40 maxDif,
		aw2 wight,
dbo.Core_CalculateBreakActDec(bfral,bfrar,aw2) as ActDc,
dbo.[Core_CalculateBreakActDif](bfral,bfrar) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
UNION ALL
SELECT 
    'Service Break - Axle 3' AS Description_En, 
    bf3l AS Left_Value, 
    bf3r AS Right_Value,
	3 as Rtype,
	id,
	45 MinDec,
	35 maxDif,
		aw3 wight,
dbo.Core_CalculateBreakActDec(bf3l,bf3r,aw3) as ActDc,
dbo.[Core_CalculateBreakActDif](bf3l,bf3r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 

SELECT 
    'Service Break - Axle 4' AS Description_En, 
    bf4l AS Left_Value, 
    bf4r AS Right_Value,
	4 as Rtype,
	id,
	35 MinDec,
	40 maxDif,
		aw4 wight,
dbo.Core_CalculateBreakActDec(bf4l,bf4r,aw4) as ActDc,
dbo.[Core_CalculateBreakActDif](bf4l,bf4r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 
SELECT 
    'Service Break - Axle 5' AS Description_En, 
    bf5l AS Left_Value, 
    bf5r AS Right_Value,
	5 as Rtype,
	id,
	45 MinDec,
	35 maxDif,
		aw5 wight,
dbo.Core_CalculateBreakActDec(bf5l,bf5r,aw5) as ActDc,
dbo.[Core_CalculateBreakActDif](bf5l,bf5r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 
SELECT 
    'Service Break - Axle 6' AS Description_En, 
    bf6l AS Left_Value, 
    bf6r AS Right_Value,
	6 as Rtype,
	id,
	35 MinDec,
	40 maxDif,
		aw6 wight,
dbo.Core_CalculateBreakActDec(bf6l,bf6r,aw6) as ActDc,
dbo.[Core_CalculateBreakActDif](bf6l,bf6r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 
SELECT 
    'Service Break - Axle 7' AS Description_En, 
    bf7l AS Left_Value, 
    bf7r AS Right_Value,
	7 as Rtype,
	id,
	45 MinDec,
	35 maxDif,
	aw7 wight,
dbo.Core_CalculateBreakActDec(bf7l,bf7r,aw7) as ActDc,
dbo.[Core_CalculateBreakActDif](bf7l,bf7r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 
SELECT 
    'Service Break - Axle 8' AS Description_En, 
    bf8l AS Left_Value, 
    bf8r AS Right_Value,
	8 as Rtype,
	id,
	35 MinDec,
	40 maxDif,
	aw8 wight,
dbo.Core_CalculateBreakActDec(bf8l,bf8r,aw8) as ActDc,
dbo.[Core_CalculateBreakActDif](bf8l,bf8r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
union all 
SELECT 
    'Service Break - Axle 9' AS Description_En, 
    bf9l AS Left_Value, 
    bf9r AS Right_Value,
	9 as Rtype,
	id,
	45 MinDec,
	35 maxDif,
	aw9 wight,
dbo.Core_CalculateBreakActDec(bf9l,bf9r,aw9) as ActDc,
dbo.[Core_CalculateBreakActDif](bf9l,bf9r) as actDif
FROM QDRIVE_FAHES_VIS_SYNC.dbo.q2d_qdrive_inspection
GO


--------------------------------------------------


