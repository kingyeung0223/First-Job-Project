-- =============================================
-- Author:		<King Yeung>
-- Create date: <2018-12-06>
-- Description:	<A store procedure that run dynamic query to convert and return the result of a survey as a true false matrix>
-- =============================================
ALTER PROCEDURE [dbo].[GenSurveyRawDataSet]
	@SurveyId as nvarchar(40),
	@QuestionTypeTable as QuestionTypeTableType readonly 
	-- @QuestionTypeTable is a table-value parameter that stores: 
	-- 1) js_path to the answer of a specfic survey question
	-- 2) a user-defined question number for display purpose
	-- 3) a bit which indicates if the question has remarks
	-- 4) a bit which indicates if the question is an open-end question
AS
BEGIN
	SET NOCOUNT ON;

	--check if survey id is valid first, if not then end the procedure
	declare @survey_exist as int
	select @survey_exist = count(1) from dbo.Survey where Id = @SurveyId
	if @survey_exist = 1
		begin
			-- A variable to store the dynamic query
			declare @sql as nvarchar(max)
				set @sql = ''

			-- Find a dumpy respondent to obtain full set of questions
			declare @dummy_respondent as nvarchar(10)
				select top 1 @dummy_respondent = s.StaffNo from dbo.SurveyStaff s where s.HasAnswered = 1

			-- A table variable for storing names and alias of all dynamically created tables
			-- The table names and alias are necessary for forming the final dynamic query string which joins all created pivot tables together
			declare @temptable_list table
			(
				name nvarchar(50),
				alias nvarchar(10),
				seq int identity
			)

			-- js_path to the answer of a specfic survey question
			declare @js_path as nvarchar(255)
			
			--a user-defined question number for display purpose
			declare @QuestionNum as nvarchar(10)
			
			-- a bit which indicates if the question has remarks
			declare @WithRemark as bit
			
			-- a bit which indicates if the question is an open-end question
			declare @IsOpenQuestion as bit

			-- This count would be used to name different dynamically created tables
			declare @temp_table_count as int
				set @temp_table_count = 0

			declare question_cursor cursor for select * from @QuestionTypeTable
			open question_cursor
			fetch next from question_cursor into @js_path, @QuestionNum, @WithRemark, @IsOpenQuestion

			while @@FETCH_STATUS = 0
			begin
				declare @table_name as nvarchar(50)
				declare @table_alias as nvarchar(4)
				declare @create_temp as nvarchar(max)
				declare @insert_into_temp as nvarchar(max)

				-- Retrieve a respondent's answer from a JSON field and store it into a dynamically created table
				if @IsOpenQuestion = 1
					begin
						-- Handling Open-end Question
						-- create a table to store the answer of each respondent
						set @table_name = N'temp_survey_table' + cast(@temp_table_count as nvarchar(3))
						set @table_alias = N'q' + cast(@temp_table_count as nvarchar(3))
						insert into @temptable_list values (@table_name, @table_alias)
						set @create_temp = N'create table ' + @table_name + ' (' + 'Respondent nvarchar(10), ' + @QuestionNum + '_Answer nvarchar(max)); '

						set @temp_table_count = @temp_table_count + 1

						-- insert respondent's answer into the above table
						set @insert_into_temp = N'insert into ' + @table_name + ' select Respondent, OpenEndAnswer from dbo.GetOpenEndAnswer( ''' + @SurveyId + ''', ''' + @js_path + '''); ' 
						
						-- Update the dynamic query string
						set @sql = @sql + @create_temp + @insert_into_temp
					end
				else
					begin
						-- Handling Multiple Choice Question
						-- A respondent's answer would be converted into a pivot table
						-- Obtain the pivot table definition based on choices of a specific MC question
						declare @temp_table_def as nvarchar(max)
						SET @temp_table_def = NULL
						select @temp_table_def = COALESCE(@temp_table_def + ' tinyint, ', '') + @QuestionNum + '_' + q.AvailableChoice 
						FROM ( 
							select AvailableChoice 
							from dbo.GetMC_AnswerAsUnpivotTrueFalseMatrix(@SurveyId, @js_path)
							where Respondent = @dummy_respondent
							) as q

						set @temp_table_def = ' Respondent nvarchar(10), ' + @temp_table_def + ' tinyint '
						set @temp_table_def = replace(@temp_table_def, '__', '_')

						-- create temp table for storing pivot results
						set @table_name = N'temp_survey_table' + cast(@temp_table_count as nvarchar(3))
						set @table_alias = N'q' + cast(@temp_table_count as nvarchar(3))
						insert into @temptable_list values (@table_name, @table_alias)
						set @create_temp = N'create table ' + @table_name + ' (' + @temp_table_def + '); '

						set @temp_table_count = @temp_table_count + 1

						-- Update the dynamic query string
						set @sql = @sql + @create_temp


						-- obtain column names for converting a respondent's answer of a MC question into a pivot table
						declare @col as nvarchar(max);
						SET @col = NULL
						select @col = COALESCE(@col + ', ', '') + q.AvailableChoice 
						FROM ( 
							select AvailableChoice 
							from dbo.GetMC_AnswerAsUnpivotTrueFalseMatrix(@SurveyId, @js_path)
							where Respondent = @dummy_respondent
							) as q

						-- pivot respondent's answer and store it into the table created above
						set @insert_into_temp = 'insert into ' + @table_name + ' select Respondent, ' + @col + ' from (select Respondent, IsSelected, AvailableChoice from dbo.GetMC_AnswerAsUnpivotTrueFalseMatrix(''' + @SurveyId + ''', ''' + @js_path + ''')) as q pivot (sum(q.IsSelected) for q.AvailableChoice in (' + @col + ')) as pvt; '
						
						-- Update the dynamic query string
						set @sql = @sql + @insert_into_temp


						-- handle questions with remark
						if @WithRemark = 1
							begin
								-- create the table to store the answer of each respondent
								set @table_name = N'temp_survey_table' + cast(@temp_table_count as nvarchar(3))
								set @table_alias = N'q' + cast(@temp_table_count as nvarchar(3))
								insert into @temptable_list values (@table_name, @table_alias)
								set @create_temp = N'create table ' + @table_name + ' (' + 'Respondent nvarchar(10), ' + @QuestionNum + '_Remarks nvarchar(max)); '

								set @temp_table_count = @temp_table_count + 1

								-- insert answer into the above table
								set @insert_into_temp = N'insert into ' + @table_name + ' ' +
									'select Respondent, 
										stuff(
												(
												select distinct '','' + Remarks 
												from dbo.GetMC_AnswerAsUnpivotTrueFalseMatrix( ''' + @SurveyId + ''', ''' + @js_path + ''') 
												where Remarks is not null and Respondent = a.Respondent
												for xml path('''')
												),
												1, 1, ''''
										)
									from dbo.GetMC_AnswerAsUnpivotTrueFalseMatrix( ''' + @SurveyId + ''', ''' + @js_path + ''') as a
									where Remarks is not null
									group by a.Respondent
									; ' 

								-- Update the dynamic query string
								set @sql = @sql + @create_temp + @insert_into_temp
							end
					end
				fetch next from question_cursor into @js_path, @QuestionNum, @WithRemark, @IsOpenQuestion
			end
			close question_cursor
			deallocate question_cursor

			--create physical tables to store raw data of different questions
			exec (@sql)


			-- form dynamic query string which joins all created tables to get full set of results
			-- after the result set is retrieved, use dynamic query again to drop all created tables
			Declare @first_alias as nvarchar(10)
			Declare @first_table_name as nvarchar(50)
			select top 1 @first_alias = alias from @temptable_list
			select top 1 @first_table_name = name from @temptable_list


			if @first_alias is not null AND @first_table_name is not null
				begin
					-- beginning part of the query string for dropping all created tables
					Declare @drop_table as nvarchar (max)
					set @drop_table = 'Drop Table ' + @first_table_name

					-- form the select clause
					Declare @select_clause as nvarchar(max)
					set @select_clause = NULL
					select @select_clause = coalesce(@select_clause + ', ', '') + c.name 
					from sys.tables t join sys.columns c on c.object_id = t.object_id 
					where t.name like 'temp_survey_table%' and c.column_id <> 1;

					set @select_clause = 'select ' + @first_alias + '.Respondent, ' + @select_clause


					--form the beginning part of the from clause
					declare @from_clause as nvarchar(max)
					set @from_clause = 'from ' + @first_table_name + ' ' + @first_alias + ' '

					-- loop through names of all created table to complete the from clause with join statement
					declare @current_table as nvarchar(53)
					declare @current_table_alias as nvarchar(4)
					declare table_cursor cursor for select name, alias from @temptable_list where seq <> 1
					open table_cursor
					fetch next from table_cursor into @current_table, @current_table_alias

					while @@FETCH_STATUS = 0
					begin
						set @from_clause = @from_clause + 'left join ' + @current_table + ' ' + @current_table_alias + ' on ' + @current_table_alias + '.Respondent = ' + @first_alias + '.Respondent '
						set @drop_table = @drop_table + ', ' + @current_table
						fetch next from table_cursor into @current_table, @current_table_alias
					end

					-- combine the select clause and from clause to perform query
					set @sql = @select_clause + ' ' + @from_clause + ';'
					set @drop_table = @drop_table + ';'
					--select @sql
					--select @drop_table
					exec(@sql)
					exec (@drop_table)
				end
		end
	else
		select 'Invalid Survey Id. Please Double Check the Survey Id'
END
