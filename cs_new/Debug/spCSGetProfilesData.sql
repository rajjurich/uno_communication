
/****** Object:  StoredProcedure [dbo].[spCSGetProfilesData]    Script Date: 09/27/2017 11:02:42 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[spCSGetProfilesData]         
 -- Add the parameters for the stored procedure here        
 @ControllerID varchar(max)        
AS        
BEGIN        
 -- SET NOCOUNT ON added to prevent extra result sets from        
 -- interfering with SELECT statements.        
 SET NOCOUNT ON;        
        
    -- Insert statements for procedure here        
 Declare @ReturnTable as table        
 (        
  EALID varchar(max),        
  CardCode varchar(max),        
  ActivationDate datetime,        
  ExpiryDate datetime,        
  PIN varchar(max),        
  EmployeeCode varchar(max),        
  ALID varchar(max),        
  ALArray varchar(max),        
  AuthMode varchar(max),        
  ProfileAction varchar(max)        
 )        
 Declare @CardCode as varchar(max) = ''        
 Declare @ALID as varchar(max) = ''        
 Declare @ProfileAction varchar(max) = ''        
 Declare @EALID varchar(max) = ''        
 Declare curMain cursor for 
 Select EAL_ID, CARD_CODE, AL_ID, FLAG from dbo.EAL_CONFIG with(nolock) where CONTROLLER_ID = @ControllerID and FLAG <> -1 and isdeleted=0     
 --select EAL_ID, EAL.CARD_CODE, AL_ID, FLAG from EAL_CONFIG EAL
 --inner join ENT_EMPLOYEE_PERSONAL_DTLS EPD on EPD.EPD_EMPID = EAL.EMPLOYEE_CODE and EPD.EPD_CARD_ID=EAL.CARD_CODE 
 --inner join ACS_CARD_CONFIG ACS on ACS.CC_EMP_ID = EAL.EMPLOYEE_CODE and ACS.CARD_CODE=EAL.CARD_CODE
 --inner join ENT_EMPLOYEE_OFFICIAL_DTLS EOD on EOD.EOD_EMPID = EAL.EMPLOYEE_CODE
 --where ISDELETED=0 and flag<>-1 and CONTROLLER_ID = @ControllerID
 --and EPD.EPD_ISDELETED=0  
 --and EOD.EOD_ACTIVE=1 
 --and ACS.ACE_isdeleted=0 
 --and (EPD.EPD_CARD_ID<>null or EPD.EPD_CARD_ID<>'')
 --and (ACS.CARD_CODE<>null or ACS.CARD_CODE<>'')



 open curMain         
 fetch next from curMain into @EALID, @CardCode, @ALID, @ProfileAction        
 while(@@Fetch_status = 0)        
 begin        
  Declare @ActivationDate as datetime        
  Declare @ExpiryDate as datetime        
  Declare @PIN as varchar(max)        
  Declare @EmployeeCode as varchar(max)        
  Declare @AuthMode as varchar(max)        
  Declare @ALArray as varchar(max) = ''   
  
 
  set @ActivationDate=null
  set  @ExpiryDate=null
  set @PIN=null
  set   @EmployeeCode =null  
   
  Select top 1 @ActivationDate = ACTIVATION_DATE, @ExpiryDate = [EXPIRY_DATE], @PIN = PIN, @EmployeeCode = CC_EMP_ID, @AuthMode = AUTH_MODE from dbo.ACS_CARD_CONFIG where Card_code = @CardCode   and ACE_isdeleted=0     
  if (@ActivationDate is null or @ExpiryDate is null  or @EmployeeCode is null)
	begin
  
		declare @desc varchar(max)
		set @desc = 'Employee code not found for card code ' +@Cardcode + ', please check ACS_CARD_CONFIG'
		declare @p4 int
		--0 for insert
		set @p4=0
		exec spInsertEventLogs @ControllerID=@ControllerID,@EventDescription=@desc,@Status='Failed',@EventLogID=@p4 output
		--select @p4
 
	end
  else
  begin
  print @EmployeeCode
  print ('Select top 1 @ActivationDate = ACTIVATION_DATE, @ExpiryDate = [EXPIRY_DATE], @PIN = PIN, @EmployeeCode = CC_EMP_ID, @AuthMode = AUTH_MODE from dbo.ACS_CARD_CONFIG where Card_code = '''+@CardCode +'''     and ACE_isdeleted=0    ')
  DECLARE @AccessLevelId AS VARCHAR(max)        
  DECLARE curALArray CURSOR FOR SELECT AL_ID FROM EAL_CONFIG WHERE EMPLOYEE_CODE = @EmployeeCode and CONTROLLER_ID = @ControllerID    
  OPEN curALArray         
  FETCH NEXT FROM curALArray INTO @AccessLevelId        
  WHILE(@@FETCH_STATUS = 0)        
  BEGIN        
   Select @ALArray = @ALArray + ',' + @AccessLevelId
   --SELECT @ALArray = @ALArray + ',' + AccesLevelArray FROM ACS_ACCESSLEVEL_RELATION WHERE AL_ID = @AccessLevelId and CONTROLLER_ID = @ControllerID        
   FETCH NEXT FROM curALArray INTO @AccessLevelId        
  END        
  CLOSE curALArray        
  DEALLOCATE curALArray        
  --Select @ALArray = AccesLevelArray from dbo.ACS_ACCESSLEVEL_RELATION where AL_ID = @ALID and CONTROLLER_ID = @ControllerID        
  Insert into @ReturnTable (EALID, CardCode, ActivationDate, ExpiryDate, PIN, EmployeeCode, ALID, ALArray, AuthMode, ProfileAction)         
  values (@EALID, @CardCode, @ActivationDate, @ExpiryDate, @PIN, @EmployeeCode, @ALID, @ALArray, @AuthMode, @ProfileAction)        
        end  
  fetch next from curMain into @EALID, @CardCode, @ALID, @ProfileAction        
 end        
 close curMain        
 Deallocate curMain        
 Select * from @ReturnTable        
END
