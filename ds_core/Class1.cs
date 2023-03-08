using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using ds_common;



namespace ds_core
{
    public static class Commands
    {






        public static ofr Distinct(ref OActivityState AState, OIncrementalDataObject IDO = null)
        {
            // Prepare the return object
            ofr ret = new ofr();
            ret.DT = DateTime.Now;
            ret.PrimaryReturnValue = RefReturnValues.Indeterminate;
            ret.FunctionName = "Distinct";
            // Set the Processing Point description
            ret.ProcessingPoint = "Initialising function";
            ret.AdditionalInfo = string.Empty;


            if (AState == null) return ret;

            const string CommandName = "distinct";

            // Check that the right command is being called
            if (string.Equals(AState.ActivityName, CommandName, StringComparison.OrdinalIgnoreCase) == false)
            {
                ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                return ret;
            }


            // Record the first engagement with the command on first pass
            if (AState.Started == false)
            {
                AState.Started = true;
                AState.StartedDT = DateTime.Now;

                ofr aofr = new ofr();

                aofr.DT = DateTime.Now;
                aofr.PrimaryReturnValue = RefReturnValues.Indeterminate;
                aofr.FunctionName = AState.ActivityName.ToString();
                aofr.ProcessingPoint = "Command started";

                AState.ofr = aofr;

            }



            //[*************** Perform command initialisation activities if required
            if (AState.HasInitialisationCompleted == false)
            {
                AState.ofr.ProcessingPoint = "Initialisation started";


                //[******** Specific initialisations
                AState.OutputObj.Information = new List<string>();
                AState.OutputObj.Information.Add(ds_common.Utilities.VisualisationType_Grid);

                AState.OutputObj.PrimaryOutput = new List<object>();


                AState.IntermediateObj = new List<object>();
                AState.IntermediateObj.Add(new List<KeyValuePair<string, long>>());  // Item 0 - the distinct values list


                // Check if the user requested that the visualisation type should be the grid
                string ct = ds_common.Utilities.GetConfigParameter(ds_common.Utilities.VisualisationMethodKeyword, ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, ds_common.Utilities.VisualisationType_Text, StringComparison.OrdinalIgnoreCase))
                    {
                        AState.OutputObj.Information[0] = ds_common.Utilities.VisualisationType_Text;
                    }
                }
                //********]


                AState.HasInitialisationCompleted = true;
                AState.ofr.ProcessingPoint = "Initialisation completed";

                if (IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.Success;
                    return ret;
                }
            }
            //***************]



            // Determine if this the incremental data processing has completed, by checking if no more data have been provided
            if (AState.HasInitialisationCompleted == true & IDO == null)
            {
                if (AState.DataExtractComplete == false)
                {
                    AState.DataExtractComplete = true;
                    AState.DataExtractCompleteDT = DateTime.Now;
                }
            }





            //[**************** If there are still more data points to accummulate then do the accummulation stuff here


            // Declare some useful constants
            const int IntObj_DistinctList = 0;


            if (AState.DataExtractComplete == false)
            {
                if (IDO == null || IDO.IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                    return ret;
                }


                int FieldID = -1;

                //[************************ Check if the mandatory parameters have been provided
                string ct = ds_common.Utilities.GetConfigParameter("field_id", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No field_id parameter specified";
                    return ret;
                }
                else
                {
                    if (int.TryParse(ct, out FieldID))
                    {
                        if (FieldID <= 0)
                        {
                            ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                            ret.AdditionalInfo = "field_id parameter has a zero or negative value";
                            return ret;
                        }
                    }
                    else
                    {
                        ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                        ret.AdditionalInfo = "Couldn't parse the field_id parameter value into an integer";
                        return ret;
                    }
                }
                //************************]






                //[***************************** Do the relevant processing


                bool CaseSensitive = false;

                ct = ds_common.Utilities.GetConfigParameter("case_sensitive", ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, "true", StringComparison.OrdinalIgnoreCase))
                    {
                        CaseSensitive = true;
                    }
                }



                if (IDO.SourceIsText)
                {
                    // Process text-based incremental data objects

                    int u;

                    // Get the fields of data
                    List<object> LFields = (List<object>)IDO.IDO;



                    if (FieldID > LFields.Count)
                    {
                        AState.ofr.PrimaryReturnValue = RefReturnValues.InvalidConfigurationSpecification;
                        ret.AdditionalInfo = "Field ID requested is outside of the range of available fields";
                    }
                    else
                    {



                        List<KeyValuePair<string, long>> LS = (List<KeyValuePair<string, long>>)AState.IntermediateObj[IntObj_DistinctList];


                        StringComparison sc = StringComparison.OrdinalIgnoreCase;
                        if (CaseSensitive)
                        {
                            sc = StringComparison.Ordinal;
                        }



                        u = LS.FindIndex(x => x.Key.Equals((string)LFields[FieldID - 1], sc));

                        if (u == -1)
                        {
                            // Create the key-value pair with the required values
                            KeyValuePair<string, long> kvp = new KeyValuePair<string, long>((string)LFields[FieldID - 1], 1);

                            // Add to the list
                            LS.Add(kvp);
                        }
                        else
                        {
                            long c = LS[u].Value;
                            c++;

                            // Create the key-value pair with the required values
                            KeyValuePair<string, long> kvp = new KeyValuePair<string, long>(LS[u].Key, c);

                            // Overwrite the old key-value pair
                            LS[u] = kvp;
                        }

                        AState.IntermediateObj[IntObj_DistinctList] = LS;
                    }

                }
                else
                {
                    // Process binary-based incremental data objects
                }


                //*****************************]



                ret.PrimaryReturnValue = RefReturnValues.Success;
                return ret;
            }
            //****************]






            //[********************** Do any final (end of incremental data provision) processing here
            if (AState.DataExtractComplete == true)
            {
                bool Cont = true;

                List<object> LGridOutput;

                // Catch the scenario where data couldn't be correctly processed due to a configuration issue
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.InvalidConfigurationSpecification)
                {
                    const string e = "Field ID used to calculate the statistics is outside of the range of available fields";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }

                // Catch the scenario where no data were accummulated to analyse
                if (AState.ProcessedRowsCount == 0)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    if (AState.AnalysisWindowUsed)
                    {
                        AState.ofr.ErrorMessage = "No values were obtained. Hints: (1) Check that you're parsing the file correctly (use :analyse_file_format{}). (2) Check that your Analysis Window is sampling the data appropriately.";
                    }
                    else
                    {
                        AState.ofr.ErrorMessage = "No values were obtained. Hint: Check that you're parsing the file correctly (use :analyse_file_format{}).";
                    }
                    Cont = false;
                }


                if (Cont)
                {
                    long c;
                    double Percent;
                    List<KeyValuePair<string, long>> LS = (List<KeyValuePair<string, long>>)AState.IntermediateObj[IntObj_DistinctList];
                    string t;

                    bool ShowProportions = true;
                    int NumOutputRows = 100;
                    const int MaxOutputRows = 1000;



                    // Check the proportion parameter
                    string ct = ds_common.Utilities.GetConfigParameter("show_proportions", ref AState);
                    if (string.IsNullOrEmpty(ct) == false)
                    {
                        if (string.Equals(ct, "false", StringComparison.OrdinalIgnoreCase))
                        {
                            ShowProportions = false;
                        }
                    }

                    // Check the max output parameter
                    ct = ds_common.Utilities.GetConfigParameter("row_limit", ref AState);
                    if (string.IsNullOrEmpty(ct) == false)
                    {
                        if (int.TryParse(ct, out NumOutputRows))
                        {
                            if (NumOutputRows > MaxOutputRows) NumOutputRows = MaxOutputRows;
                        }
                    }

                    // Check the sort output parameter
                    ct = ds_common.Utilities.GetConfigParameter("sort_output", ref AState);
                    if (string.IsNullOrEmpty(ct) == false)
                    {
                        switch (ct.ToLowerInvariant())
                        {
                            case "ascending":
                                LS = LS.OrderBy(x => x.Value).ToList();
                                break;
                            case "descending":
                                LS = LS.OrderByDescending(x => x.Value).ToList();
                                break;
                        }
                    }



                    //[********************************************** Create outputs
                    if (AState.OutputObj.Information[0] == ds_common.Utilities.VisualisationType_Grid)
                    {
                        // Set the grid headers appropriately
                        AState.SourceOriginalFieldHeaders = new List<string>();
                        AState.SourceOriginalFieldHeaders.Add("Value");
                        if (ShowProportions)
                        {
                            AState.SourceOriginalFieldHeaders.Add("Count");
                            AState.SourceOriginalFieldHeaders.Add("Percent");
                        }

                        // Loop over the distinct values
                        for (int i = 0; i < LS.Count; i++)
                        {
                            if (i >= NumOutputRows) break;

                            LGridOutput = new List<object>();

                            LGridOutput.Add((object)LS[i].Key);

                            if (ShowProportions)
                            {
                                c = LS[i].Value;
                                LGridOutput.Add((object)c.ToString());
                                Percent = ((double)c / (double)AState.ProcessedRowsCount) * 100.0d;
                                LGridOutput.Add((object)Percent.ToString());
                            }

                            AState.OutputObj.PrimaryOutput.Add(LGridOutput);
                        }


                        // Note on Analysis Window
                        if (ShowProportions & AState.AnalysisWindowUsed)
                        {
                            LGridOutput = new List<object>();
                            t = "Note: an Analysis Window was used, so the percentage values may not reflect the proportion of the total number of rows in the source";
                            LGridOutput.Add((object)t);
                            if (ShowProportions)
                            {
                                LGridOutput.Add((object)string.Empty);
                                LGridOutput.Add((object)string.Empty);
                                AState.OutputObj.PrimaryOutput.Add(LGridOutput);
                            }
                        }
                    }
                    else
                    {

                        t = "Number of distict values:\t" + LS.Count.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);
                        AState.OutputObj.PrimaryOutput.Add(string.Empty);

                        t = "Value";
                        if (ShowProportions) t += "\tCount\tPercent";
                        AState.OutputObj.PrimaryOutput.Add(t);


                        for (int i = 0; i < LS.Count; i++)
                        {
                            if (i >= NumOutputRows) break;

                            t = LS[i].Key;
                            if (ShowProportions)
                            {
                                c = LS[i].Value;
                                Percent = ((double)c / (double)AState.ProcessedRowsCount) * 100.0d;

                                t += "\t" + c.ToString() + "\t" + Percent;
                            }

                            AState.OutputObj.PrimaryOutput.Add(t);
                        }


                        // Note on Analysis Window
                        if (ShowProportions & AState.AnalysisWindowUsed)
                        {
                            AState.OutputObj.PrimaryOutput.Add(string.Empty);
                            t = "Note: an Analysis Window was used, so the percentage values may not be the proportion of the total number of rows in the source";
                            AState.OutputObj.PrimaryOutput.Add(t);
                        }
                    }
                    //**********************************************]


                }
            }
            //**********************]




            // Do the necessary things here to display the outputs

            //if (AState.ofr.PrimaryReturnValue == RefReturnValues.Success)
            //{
            //    // Return the output as the answer

            //}
            //else
            //{
            //    // Return the output as the error message, etc
            //}




            AState.Completed = true;
            AState.CompletedDT = DateTime.Now;


            // If no other return value has been set, then set one of success
            if (AState.ofr.PrimaryReturnValue == RefReturnValues.Indeterminate) AState.ofr.PrimaryReturnValue = RefReturnValues.Success;



            ret.PrimaryReturnValue = RefReturnValues.Success;
            return ret;
        }
















        public static ofr Count(ref OActivityState AState, OIncrementalDataObject IDO = null)
        {
            // Prepare the return object
            ofr ret = new ofr();
            ret.DT = DateTime.Now;
            ret.PrimaryReturnValue = RefReturnValues.Indeterminate;
            ret.FunctionName = "Count";
            // Set the Processing Point description
            ret.ProcessingPoint = "Initialising function";
            ret.AdditionalInfo = string.Empty;


            if (AState == null) return ret;

            const string CommandName = "count";

            // Check that the right command is being called
            if (string.Equals(AState.ActivityName, CommandName, StringComparison.OrdinalIgnoreCase) == false)
            {
                ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                return ret;
            }


            // Record the first engagement with the command on first pass
            if (AState.Started == false)
            {
                AState.Started = true;
                AState.StartedDT = DateTime.Now;

                ofr aofr = new ofr();

                aofr.DT = DateTime.Now;
                aofr.PrimaryReturnValue = RefReturnValues.Indeterminate;
                aofr.FunctionName = AState.ActivityName.ToString();
                aofr.ProcessingPoint = "Command started";

                AState.ofr = aofr;

            }



            //[*************** Perform command initialisation activities if required
            if (AState.HasInitialisationCompleted == false)
            {
                AState.ofr.ProcessingPoint = "Initialisation started";


                //[******** Specific initialisations
                AState.OutputObj.Information = new List<string>();
                AState.OutputObj.Information.Add(ds_common.Utilities.VisualisationType_Text);

                AState.OutputObj.PrimaryOutput = new List<object>();

                long tc = 0;

                AState.IntermediateObj = new List<object>();
                AState.IntermediateObj.Add(tc);  // Item 0 - the main counter
                AState.IntermediateObj.Add(new List<string>());  // Item 1 - the distinct values list


                // Check if the user requested that the visualisation type should be the grid
                string ct = ds_common.Utilities.GetConfigParameter(ds_common.Utilities.VisualisationMethodKeyword, ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, ds_common.Utilities.VisualisationType_Grid, StringComparison.OrdinalIgnoreCase))
                    {
                        AState.OutputObj.Information[0] = ds_common.Utilities.VisualisationType_Grid;
                    }
                }
                //********]


                AState.HasInitialisationCompleted = true;
                AState.ofr.ProcessingPoint = "Initialisation completed";

                if (IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.Success;
                    return ret;
                }
            }
            //***************]



            // Determine if this the incremental data processing has completed, by checking if no more data have been provided
            if (AState.HasInitialisationCompleted == true & IDO == null)
            {
                if (AState.DataExtractComplete == false)
                {
                    AState.DataExtractComplete = true;
                    AState.DataExtractCompleteDT = DateTime.Now;
                }
            }





            //[**************** If there are still more data points to accummulate then do the accummulation stuff here


            // Declare some useful constants
            const int IntObj_Count = 0;
            const int IntObj_DistinctList = 1;


            if (AState.DataExtractComplete == false)
            {
                if (IDO == null || IDO.IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                    return ret;
                }


                string ct;
                int FieldID = -1;

                //[************************ Check if the mandatory parameters have been provided
                ct = ds_common.Utilities.GetConfigParameter("field_id", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No field_id parameter specified";
                    return ret;
                }
                else
                {
                    if (int.TryParse(ct, out FieldID))
                    {
                        if (FieldID <= 0)
                        {
                            ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                            ret.AdditionalInfo = "field_id parameter has a zero or negative value";
                            return ret;
                        }
                    }
                    else
                    {
                        ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                        ret.AdditionalInfo = "Couldn't parse the field_id parameter value into an integer";
                        return ret;
                    }
                }
                //************************]






                //[***************************** Do the relevant processing


                string CountType = string.Empty;

                ct = ds_common.Utilities.GetConfigParameter("option", ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    CountType = ct;
                }



                if (IDO.SourceIsText)
                {
                    // Process text-based incremental data objects

                    long c = 0;

                    // Get the fields of data
                    List<object> LFields = (List<object>)IDO.IDO;



                    if (FieldID > LFields.Count)
                    {
                        AState.ofr.PrimaryReturnValue = RefReturnValues.InvalidConfigurationSpecification;
                        ret.AdditionalInfo = "Field ID requested is outside of the range of available fields";
                    }
                    else
                    {


                        if (string.IsNullOrEmpty(CountType))
                        {
                            c = (long)AState.IntermediateObj[IntObj_Count];
                            c++;
                            AState.IntermediateObj[IntObj_Count] = c;
                        }
                        else
                        {
                            switch (CountType.ToLowerInvariant())
                            {
                                case "distinct":
                                    List<string> LS = (List<string>)AState.IntermediateObj[IntObj_DistinctList];
                                    if (LS.Contains((string)LFields[FieldID - 1]) == false)
                                    {
                                        // Add the new item to the list
                                        LS.Add((string)LFields[FieldID - 1]);

                                        // Handle scenarios where the number of items might be larger than the 32-bit limit
                                        if (AState.ProcessedRowsCount >= int.MaxValue)
                                        {
                                            c = LS.LongCount();
                                        }
                                        else
                                        {
                                            c = LS.Count;
                                        }

                                        AState.IntermediateObj[IntObj_Count] = c;
                                    }
                                    break;

                                case "empty":
                                    c = (long)AState.IntermediateObj[IntObj_Count];

                                    if (string.IsNullOrEmpty((string)LFields[FieldID - 1]))
                                    {
                                        c++;
                                        AState.IntermediateObj[IntObj_Count] = c;
                                    }
                                    break;

                                case "not_empty":
                                    c = (long)AState.IntermediateObj[IntObj_Count];

                                    if (string.IsNullOrEmpty((string)LFields[FieldID - 1]) == false)
                                    {
                                        c++;
                                        AState.IntermediateObj[IntObj_Count] = c;
                                    }
                                    break;

                                case "zero":
                                    c = (long)AState.IntermediateObj[IntObj_Count];
                                    double nv;

                                    if (double.TryParse((string)LFields[FieldID - 1], out nv))
                                    {
                                        if (nv == 0.0d)
                                        {
                                            c++;
                                            AState.IntermediateObj[IntObj_Count] = c;
                                        }
                                    }
                                    break;

                                default:
                                    c = (long)AState.IntermediateObj[IntObj_Count];
                                    c++;
                                    AState.IntermediateObj[IntObj_Count] = c;
                                    break;
                            }
                        }

                    }

                }
                else
                {
                    // Process binary-based incremental data objects
                }


                //*****************************]



                ret.PrimaryReturnValue = RefReturnValues.Success;
                return ret;
            }
            //****************]






            //[********************** Do any final (end of incremental data provision) processing here
            if (AState.DataExtractComplete == true)
            {
                bool Cont = true;

                List<object> LGridOutput;

                // Catch the scenario where data couldn't be correctly processed due to a configuration issue
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.InvalidConfigurationSpecification)
                {
                    const string e = "Field ID used to calculate the statistics is outside of the range of available fields";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }

                // Catch the scenario where no data were accummulated to analyse
                if (AState.ProcessedRowsCount == 0)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    if (AState.AnalysisWindowUsed)
                    {
                        AState.ofr.ErrorMessage = "No values were obtained to count. Hints: (1) Check that you're parsing the file correctly (use :analyse_file_format{}). (2) Check that your Analysis Window is sampling the data appropriately.";
                    }
                    else
                    {
                        AState.ofr.ErrorMessage = "No values were obtained to count. Hint: Check that you're parsing the file correctly (use :analyse_file_format{}).";
                    }
                    Cont = false;
                }


                if (Cont)
                {
                    string t = "Count";

                    long c = (long)AState.IntermediateObj[IntObj_Count];
                    double Percent = ((double)c / (double)AState.ProcessedRowsCount) * 100.0d;



                    string CountType = string.Empty;
                    string ct;
                    ct = ds_common.Utilities.GetConfigParameter("option", ref AState);
                    if (string.IsNullOrEmpty(ct) == false)
                    {
                        CountType = ct;
                    }



                    if (string.IsNullOrEmpty(CountType) == false)
                    {
                        switch (CountType.ToLowerInvariant())
                        {
                            case "distinct":
                                t = "Count distinct values";
                                break;

                            case "empty":
                                t = "Count empty values";
                                break;

                            case "not_empty":
                                t = "Count non-empty values";
                                break;

                            case "zero":
                                t = "Count zeros";
                                break;
                        }
                    }




                    //[********************************************** Create outputs
                    if (AState.OutputObj.Information[0] == ds_common.Utilities.VisualisationType_Grid)
                    {
                        // Set the grid headers appropriately
                        AState.SourceOriginalFieldHeaders = new List<string>();
                        AState.SourceOriginalFieldHeaders.Add("Metric");
                        AState.SourceOriginalFieldHeaders.Add("Value");

                        // Count
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)t);
                        LGridOutput.Add((object)c.ToString());
                        AState.OutputObj.PrimaryOutput.Add(LGridOutput);

                        // Processed rows
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Processed rows");
                        LGridOutput.Add((object)AState.ProcessedRowsCount.ToString());
                        AState.OutputObj.PrimaryOutput.Add(LGridOutput);

                        // Percent
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Percent");
                        LGridOutput.Add((object)Percent.ToString());
                        AState.OutputObj.PrimaryOutput.Add(LGridOutput);

                        // Note on Analysis Window
                        if (AState.AnalysisWindowUsed)
                        {
                            LGridOutput = new List<object>();
                            LGridOutput.Add((object)"Note: an Analysis Window was used, so the processed rows count may not equal the total number of rows in the source");
                            LGridOutput.Add((object)string.Empty);
                            AState.OutputObj.PrimaryOutput.Add(LGridOutput);
                        }
                    }
                    else
                    {
                        // Count
                        t = t + "\t" + c.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Processed rows
                        t = "Processed rows\t" + AState.ProcessedRowsCount.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Percent
                        t = "Percent\t" + Percent.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Note on Analysis Window
                        if (AState.AnalysisWindowUsed)
                        {
                            t = "Note: an Analysis Window was used, so the processed rows count may not equal the total number of rows in the source";
                            AState.OutputObj.PrimaryOutput.Add(t);
                        }
                    }
                    //**********************************************]


                }
            }
            //**********************]




            // Do the necessary things here to display the outputs

            //if (AState.ofr.PrimaryReturnValue == RefReturnValues.Success)
            //{
            //    // Return the output as the answer

            //}
            //else
            //{
            //    // Return the output as the error message, etc
            //}




            AState.Completed = true;
            AState.CompletedDT = DateTime.Now;


            // If no other return value has been set, then set one of success
            if (AState.ofr.PrimaryReturnValue == RefReturnValues.Indeterminate) AState.ofr.PrimaryReturnValue = RefReturnValues.Success;



            ret.PrimaryReturnValue = RefReturnValues.Success;
            return ret;
        }





















        public static ofr Common_Stats(ref OActivityState AState, OIncrementalDataObject IDO = null)
        {
            // Prepare the return object
            ofr ret = new ofr();
            ret.DT = DateTime.Now;
            ret.PrimaryReturnValue = RefReturnValues.Indeterminate;
            ret.FunctionName = "Common_Stats";
            // Set the Processing Point description
            ret.ProcessingPoint = "Initialising function";
            ret.AdditionalInfo = string.Empty;


            if (AState == null) return ret;

            const string CommandName = "common_stats";

            // Check that the right command is being called
            if (string.Equals(AState.ActivityName, CommandName, StringComparison.OrdinalIgnoreCase) == false)
            {
                ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                return ret;
            }


            // Record the first engagement with the command on first pass
            if (AState.Started == false)
            {
                AState.Started = true;
                AState.StartedDT = DateTime.Now;

                ofr aofr = new ofr();

                aofr.DT = DateTime.Now;
                aofr.PrimaryReturnValue = RefReturnValues.Indeterminate;
                aofr.FunctionName = AState.ActivityName.ToString();
                aofr.ProcessingPoint = "Command started";

                AState.ofr = aofr;

            }



            //[*************** Perform command initialisation activities if required
            if (AState.HasInitialisationCompleted == false)
            {
                AState.ofr.ProcessingPoint = "Initialisation started";


                //[******** Specific initialisations
                AState.OutputObj.Information = new List<string>();
                AState.OutputObj.Information.Add(ds_common.Utilities.VisualisationType_Grid);

                AState.OutputObj.PrimaryOutput = new List<object>();

                AState.IntermediateObj = new List<object>();
                AState.IntermediateObj.Add(new List<double>());  // Item 0 - the full set of values
                AState.IntermediateObj.Add(new double());  // Item 1 - the minimum value
                AState.IntermediateObj.Add(new double());  // Item 2 - the maximum value
                AState.IntermediateObj.Add(new double());  // Item 3 - the running total


                // Check if the user requested that the visualisation type should be the grid
                string ct = ds_common.Utilities.GetConfigParameter(ds_common.Utilities.VisualisationMethodKeyword, ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, ds_common.Utilities.VisualisationType_Text, StringComparison.OrdinalIgnoreCase))
                    {
                        AState.OutputObj.Information[0] = ds_common.Utilities.VisualisationType_Text;
                    }
                }
                //********]


                AState.HasInitialisationCompleted = true;
                AState.ofr.ProcessingPoint = "Initialisation completed";

                if (IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.Success;
                    return ret;
                }
            }
            //***************]



            // Determine if this the incremental data processing has completed, by checking if no more data have been provided
            if (AState.HasInitialisationCompleted == true & IDO == null)
            {
                if (AState.DataExtractComplete == false)
                {
                    AState.DataExtractComplete = true;
                    AState.DataExtractCompleteDT = DateTime.Now;
                }
            }





            //[**************** If there are still more data points to accummulate then do the accummulation stuff here

            // Declare some useful constants
            const int IntObj_Values = 0;
            const int IntObj_Min = 1;
            const int IntObj_Max = 2;
            const int IntObj_RunningSum = 3;


            if (AState.DataExtractComplete == false)
            {
                if (IDO == null || IDO.IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                    return ret;
                }


                string ct;
                int FieldID = -1;


                //[************************ Check if the mandatory parameters have been provided

                ct = ds_common.Utilities.GetConfigParameter("field_id", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No field_id parameter specified";
                    return ret;
                }
                else
                {
                    if (int.TryParse(ct, out FieldID))
                    {
                        if (FieldID <= 0)
                        {
                            ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                            ret.AdditionalInfo = "field_id parameter has a zero or negative value";
                            return ret;
                        }
                    }
                    else
                    {
                        ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                        ret.AdditionalInfo = "Couldn't parse the field_id parameter value into an integer";
                        return ret;
                    }
                }
                //************************]



                //[***************************** Do the relevant processing
                if (IDO.SourceIsText)
                {
                    // Process text-based incremental data objects

                    double v;
                    List<double> ld;


                    List<object> LFields = (List<object>)IDO.IDO;



                    if (FieldID > LFields.Count)
                    {
                        AState.ofr.PrimaryReturnValue = RefReturnValues.InvalidConfigurationSpecification;
                        ret.AdditionalInfo = "Field ID used to calculate the arithmetic mean is outside of the range of available fields";
                    }
                    else
                    {
                        if (double.TryParse((string)LFields[FieldID - 1], out v))
                        {
                            ld = (List<double>)AState.IntermediateObj[IntObj_Values];
                            ld.Add(v);
                            AState.IntermediateObj[IntObj_Values] = ld;

                            if (((List<double>)AState.IntermediateObj[IntObj_Values]).Count == 1)
                            {
                                // Min value
                                AState.IntermediateObj[IntObj_Min] = v;

                                // Max value
                                AState.IntermediateObj[IntObj_Max] = v;

                                // Running total
                                AState.IntermediateObj[IntObj_RunningSum] = v;
                            }
                            else
                            {
                                // Min value
                                if (v < (double)AState.IntermediateObj[1]) AState.IntermediateObj[IntObj_Min] = v;

                                // Max value
                                if (v > (double)AState.IntermediateObj[2]) AState.IntermediateObj[IntObj_Max] = v;

                                // Running total
                                if (double.MaxValue - v <= (double)AState.IntermediateObj[IntObj_RunningSum])
                                {
                                    AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                }
                                else
                                {
                                    AState.IntermediateObj[IntObj_RunningSum] = (double)AState.IntermediateObj[IntObj_RunningSum] + v;
                                }
                            }




                        }
                    }

                }
                else
                {
                    // Process binary-based incremental data objects
                }


                //*****************************]



                ret.PrimaryReturnValue = RefReturnValues.Success;
                return ret;
            }
            //****************]






            //[********************** Do any final (end of incremental data provision) processing here
            if (AState.DataExtractComplete == true)
            {
                bool Cont = true;

                List<object> LGridOutput;

                List<double> ld = (List<double>)AState.IntermediateObj[IntObj_Values];

                // Catch the scenario where data couldn't be correctly processed due to a configuration issue
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.InvalidConfigurationSpecification)
                {
                    const string e = "Field ID used to calculate the statistics is outside of the range of available fields";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }

                // Catch the scenario where no data were accummulated to analyse
                if (ld.Count == 0)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    AState.ofr.ErrorMessage = "No values were obtained on which to calculate statistics";
                    Cont = false;
                }

                if (ld.Count == 1)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    AState.ofr.ErrorMessage = "No values were obtained on which to calculate statistics";
                    Cont = false;
                }

                if (Cont)
                {
                    string t;
                    double Mean;
                    double Median;
                    double StDev;
                    double StErr;
                    double StErrPercent = 0.0d;
                    bool FoundStErrPercent = true;



                    //[******************************** Calculate Median
                    Median = 0.0d;
                    long ldcount = ld.Count;
                    string ct;
                    bool Foundmv = false;
                    ct = ds_common.Utilities.GetConfigParameter("interpolation_method", ref AState);
                    if (ct == null) ct = string.Empty;


                    // Sort the list
                    ld.Sort();


                    if (ldcount % 2 == 0)
                    {
                        switch (ct.ToLowerInvariant())
                        {
                            case "none":
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                AState.OutputMessagesToUser.Add("No median was calculated because the command configuration was set to not interpolate if there is an even number of elements, which there are.");
                                break;

                            case "choose lower":
                                Median = ld[((int)(ldcount / 2.0d) - 1)];
                                Foundmv = true;
                                break;

                            case "choose higher":
                                Median = ld[(((int)(ldcount / 2.0d) - 1) + 1)];
                                Foundmv = true;
                                break;

                            case "nearest neighbour average":
                                Median = ((ld[(((int)(ldcount / 2.0d) - 1) + 1)]) + (ld[((int)(ldcount / 2.0d) - 1)])) / 2.0d;
                                Foundmv = true;
                                break;

                            case "local neighbourhood quadratic":
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                AState.OutputMessagesToUser.Add("The \"LocalNeighbourhoodQuadratic\" configuration for median calculation is not currently supported.");
                                break;

                            default:
                                // Use nearest neighbour average if no method is specified
                                Median = ((ld[(((int)(ldcount / 2.0d) - 1) + 1)]) + (ld[((int)(ldcount / 2.0d) - 1)])) / 2.0d;
                                Foundmv = true;
                                //AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                //AState.OutputMessagesToUser.Add("No valid interpolation method was specified and there was an even number of elements.");
                                break;
                        }
                    }
                    else
                    {
                        Median = ld[((int)(ldcount / 2.0d) - 1) + 1];
                        Foundmv = true;
                    }
                    //********************************]





                    //[***************** Calculate Mean
                    Mean = (double)AState.IntermediateObj[IntObj_RunningSum] / (double)ldcount;
                    //*****************]






                    //[***************** Calculate Standard Deviation
                    StDev = 0.0d;

                    for (int i = 0; i < ldcount; i++)
                    {
                        StDev += Math.Pow((ld[i] - Mean), 2.0d);
                    }

                    long BesselCorrection = 0;
                    ct = ds_common.Utilities.GetConfigParameter("bessel_correction", ref AState);
                    if (ct == null) ct = string.Empty;

                    if (string.Equals(ct, "true", StringComparison.OrdinalIgnoreCase))
                    {
                        BesselCorrection = 1;
                    }

                    StDev /= (double)(ldcount - BesselCorrection);
                    //*****************]



                    //[***************** Calculate Standard Error
                    StErr = StDev / Math.Sqrt((double)ldcount);
                    //*****************]


                    //[***************** Calculate Standard Error Percent
                    if (Mean != 0.0d)
                    {
                        StErrPercent = Math.Abs((StErr / Mean) * 100.0d);
                    }
                    else
                    {
                        FoundStErrPercent = false;
                    }
                    //*****************]





                    //[********************************************** Create outputs
                    if (AState.OutputObj.Information[0] == ds_common.Utilities.VisualisationType_Grid)
                    {
                        // Set the grid headers appropriately
                        AState.SourceOriginalFieldHeaders = new List<string>();
                        AState.SourceOriginalFieldHeaders.Add("Metric");
                        AState.SourceOriginalFieldHeaders.Add("Value");

                        // Count
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Count");
                        LGridOutput.Add((object)ldcount.ToString());
                        AState.OutputObj.PrimaryOutput.Add(LGridOutput);

                        // Min value
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Min");
                        LGridOutput.Add((object)((double)AState.IntermediateObj[IntObj_Min]).ToString());
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);

                        if (Foundmv)
                        {
                            LGridOutput = new List<object>();
                            LGridOutput.Add((object)"Median");
                            LGridOutput.Add((object)Median.ToString());
                            AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);
                        }

                        // Mean
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Mean");
                        LGridOutput.Add((object)Mean.ToString());
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);

                        // Max value
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Max");
                        LGridOutput.Add((object)((double)AState.IntermediateObj[IntObj_Max]).ToString());
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);

                        // Standard Deviation
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Standard Deviation");
                        LGridOutput.Add((object)StDev.ToString());
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);

                        // Standard Error
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Standard Error");
                        LGridOutput.Add((object)StErr.ToString());
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);

                        // Standard Error Percent
                        LGridOutput = new List<object>();
                        LGridOutput.Add((object)"Standard Error %");
                        if (FoundStErrPercent)
                        {
                            LGridOutput.Add((object)StErrPercent.ToString());
                        }
                        else
                        {
                            LGridOutput.Add((object)"Couldn't calculate because mean is zero");
                        }
                        AState.OutputObj.PrimaryOutput.Add((object)LGridOutput);
                    }
                    else
                    {
                        // Count
                        t = "Count\t" + ldcount.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Min value
                        t = "Min\t" + ((double)AState.IntermediateObj[IntObj_Min]).ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Median
                        if (Foundmv)
                        {
                            t = "Median\t" + Median.ToString();
                            AState.OutputObj.PrimaryOutput.Add(t);
                        }

                        // Mean
                        t = "Mean\t" + Mean.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Max value
                        t = "Max\t" + ((double)AState.IntermediateObj[IntObj_Max]).ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Standard Deviation
                        t = "Standard Deviation\t" + StDev.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Standard Error
                        t = "Standard Error\t" + StErr.ToString();
                        AState.OutputObj.PrimaryOutput.Add(t);

                        // Standard Error Percent
                        if (FoundStErrPercent)
                        {
                            t = "Standard Error %\t" + StErrPercent.ToString();
                        }
                        else
                        {
                            t = "Standard Error %\tCouldn't calculate because mean is zero";
                        }
                        AState.OutputObj.PrimaryOutput.Add(t);
                    }
                    //**********************************************]


                }
            }
            //**********************]




            // Do the necessary things here to display the outputs

            //if (AState.ofr.PrimaryReturnValue == RefReturnValues.Success)
            //{
            //    // Return the output as the answer

            //}
            //else
            //{
            //    // Return the output as the error message, etc
            //}




            AState.Completed = true;
            AState.CompletedDT = DateTime.Now;


            // If no other return value has been set, then set one of success
            if (AState.ofr.PrimaryReturnValue == RefReturnValues.Indeterminate) AState.ofr.PrimaryReturnValue = RefReturnValues.Success;



            ret.PrimaryReturnValue = RefReturnValues.Success;
            return ret;
        }





















        public static ofr Mean(ref OActivityState AState, OIncrementalDataObject IDO = null)
        {
            // Prepare the return object
            ofr ret = new ofr();
            ret.DT = DateTime.Now;
            ret.PrimaryReturnValue = RefReturnValues.Indeterminate;
            ret.FunctionName = "RunCommand_ANALYSE_Arithmetic_ArithmeticMean";
            // Set the Processing Point description
            ret.ProcessingPoint = "Initialising function";
            ret.AdditionalInfo = string.Empty;


            if (AState == null) return ret;

            const string CommandName = "mean";

            // Check that the right command is being called
            if (string.Equals(AState.ActivityName, CommandName, StringComparison.OrdinalIgnoreCase) == false)
            {
                ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                return ret;
            }


            // Record the first engagement with the command on first pass
            if (AState.Started == false)
            {
                AState.Started = true;
                AState.StartedDT = DateTime.Now;

                ofr aofr = new ofr();

                aofr.DT = DateTime.Now;
                aofr.PrimaryReturnValue = RefReturnValues.Indeterminate;
                aofr.FunctionName = AState.ActivityName.ToString();
                aofr.ProcessingPoint = "Command started";

                AState.ofr = aofr;

            }



            //[*************** Perform command initialisation activities if required
            if (AState.HasInitialisationCompleted == false)
            {
                AState.ofr.ProcessingPoint = "Initialisation started";


                //[******** Specific initialisations
                AState.OutputObj.Information = new List<string>();
                AState.OutputObj.Information.Add(ds_common.Utilities.VisualisationType_Text);

                AState.IntermediateObj = new List<object>();

                AState.OutputObj.PrimaryOutput = new List<object>();

                double RunningSum = 0.0d;
                long Count = 0;

                AState.IntermediateObj.Add((object)RunningSum);  // Note that this will be index 0 in the list
                AState.IntermediateObj.Add((object)Count);  // Note that this will be index 1 in the list

                // Check if the user requested that the visualisation type should be the grid
                string ct = ds_common.Utilities.GetConfigParameter(ds_common.Utilities.VisualisationMethodKeyword, ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, ds_common.Utilities.VisualisationType_Grid, StringComparison.OrdinalIgnoreCase))
                    {
                        AState.OutputObj.Information[0] = ds_common.Utilities.VisualisationType_Grid;
                    }
                }
                //********]


                AState.HasInitialisationCompleted = true;
                AState.ofr.ProcessingPoint = "Initialisation completed";

                if (IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.Success;
                    return ret;
                }
            }
            //***************]



            // Determine if this the incremental data processing has completed, by checking if no more data have been provided
            if (AState.HasInitialisationCompleted == true & IDO == null)
            {
                if (AState.DataExtractComplete == false)
                {
                    AState.DataExtractComplete = true;
                    AState.DataExtractCompleteDT = DateTime.Now;
                }
            }





            //[**************** If there are still more data points to accummulate then do the accummulation stuff here
            if (AState.DataExtractComplete == false)
            {
                if (IDO == null || IDO.IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                    return ret;
                }


                string ct;
                int FieldID = -1;

                //[************************ Check if the mandatory parameters have been provided

                ct = ds_common.Utilities.GetConfigParameter("field_id", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No field_id parameter specified";
                    return ret;
                }
                else
                {
                    if (int.TryParse(ct, out FieldID))
                    {
                        if (FieldID <= 0)
                        {
                            ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                            ret.AdditionalInfo = "field_id parameter has a zero or negative value";
                            return ret;
                        }
                    }
                    else
                    {
                        ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                        ret.AdditionalInfo = "Couldn't parse the field_id parameter value into an integer";
                        return ret;
                    }
                }
                //************************]



                //[***************************** Do the relevant processing
                if (IDO.SourceIsText)
                {
                    // Process text-based incremental data objects
                    double v;
                    double rs;
                    long c;

                    List<object> LFields = (List<object>)IDO.IDO;





                    if (FieldID > LFields.Count || FieldID <= 0)
                    {
                        AState.ofr.PrimaryReturnValue = RefReturnValues.InvalidConfigurationSpecification;
                    }
                    else
                    {
                        if (double.TryParse((string)LFields[FieldID - 1], out v))
                        {
                            // Update the running sum
                            rs = (double)AState.IntermediateObj[0];
                            if (double.MaxValue - v <= rs)
                            {
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                            }
                            else
                            {
                                rs += v;
                                AState.IntermediateObj[0] = (object)rs;
                            }

                            // Update the count
                            c = (long)AState.IntermediateObj[1];
                            if (c == long.MaxValue - 1)
                            {
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                            }
                            else
                            {
                                c++;
                                AState.IntermediateObj[1] = (object)c;
                            }

                        }
                    }

                }
                else
                {
                    // Process binary-based incremental data objects
                }


                //*****************************]



                ret.PrimaryReturnValue = RefReturnValues.Success;
                return ret;
            }
            //****************]






            //[********************** Do any final (end of incremental data provision) processing here
            if (AState.DataExtractComplete == true)
            {
                bool Cont = true;


                // Catch the scenario where data couldn't be correctly processed due to a configuration issue
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.InvalidConfigurationSpecification)
                {
                    const string e = "Field ID used to calculate the arithmetic mean is outside of the range of available fields";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }

                // Catch the scenario where the sum or count were so large that they would exceed the calculation limits
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.OtherError)
                {
                    const string e = "The numbers being averaged where either too numerous or to large in magnitude that they would have exceeded the 64 bit calculation limits of integer or double precision numbers (respectively).";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }


                // Catch the scenario where no data where accummulated to analyse
                if ((long)AState.IntermediateObj[1] == 0)
                {
                    const string e = "No values were obtained to calculate an arithmetic mean over";
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }


                if (Cont)
                {
                    // Calculate the arithmetic mean
                    double amv = ((double)AState.IntermediateObj[0]) / ((double)((long)AState.IntermediateObj[1]));


                    if (AState.OutputObj.Information[0] == ds_common.Utilities.VisualisationType_Grid)
                    {
                        // Set the grid headers appropriately
                        AState.SourceOriginalFieldHeaders = new List<string>();

                        // Set the output value
                        AState.OutputObj.PrimaryOutput.Add(new List<object>() { (object)amv.ToString() });
                    }
                    else
                    {
                        AState.OutputObj.PrimaryOutput.Add((object)amv.ToString());
                    }
                }
            }
            //**********************]




            // Do the necessary things here to display the outputs

            //if (AState.ofr.PrimaryReturnValue == RefReturnValues.Success)
            //{
            //    // Return the output as the answer

            //}
            //else
            //{
            //    // Return the output as the error message, etc
            //}




            AState.Completed = true;
            AState.CompletedDT = DateTime.Now;


            // If no other return value has been set, then set one of success
            if (AState.ofr.PrimaryReturnValue == RefReturnValues.Indeterminate) AState.ofr.PrimaryReturnValue = RefReturnValues.Success;


            ret.PrimaryReturnValue = RefReturnValues.Success;
            return ret;
        }



























        public static ofr Median(ref OActivityState AState, OIncrementalDataObject IDO = null)
        {
            // Prepare the return object
            ofr ret = new ofr();
            ret.DT = DateTime.Now;
            ret.PrimaryReturnValue = RefReturnValues.Indeterminate;
            ret.FunctionName = "Median";
            // Set the Processing Point description
            ret.ProcessingPoint = "Initialising function";
            ret.AdditionalInfo = string.Empty;


            if (AState == null) return ret;

            const string CommandName = "median";

            // Check that the right command is being called
            if (string.Equals(AState.ActivityName, CommandName, StringComparison.OrdinalIgnoreCase) == false)
            {
                ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                return ret;
            }


            // Record the first engagement with the command on first pass
            if (AState.Started == false)
            {
                AState.Started = true;
                AState.StartedDT = DateTime.Now;

                ofr aofr = new ofr();

                aofr.DT = DateTime.Now;
                aofr.PrimaryReturnValue = RefReturnValues.Indeterminate;
                aofr.FunctionName = AState.ActivityName.ToString();
                aofr.ProcessingPoint = "Command started";

                AState.ofr = aofr;

            }



            //[*************** Perform command initialisation activities if required
            if (AState.HasInitialisationCompleted == false)
            {
                AState.ofr.ProcessingPoint = "Initialisation started";


                //[******** Specific initialisations
                AState.OutputObj.Information = new List<string>();
                AState.OutputObj.Information.Add(ds_common.Utilities.VisualisationType_Text);

                AState.OutputObj.PrimaryOutput = new List<object>();

                AState.IntermediateObj = new List<object>();
                AState.IntermediateObj.Add(new List<double>());  // This is item 0

                // Check if the user requested that the visualisation type should be the grid
                string ct = ds_common.Utilities.GetConfigParameter(ds_common.Utilities.VisualisationMethodKeyword, ref AState);
                if (string.IsNullOrEmpty(ct) == false)
                {
                    if (string.Equals(ct, ds_common.Utilities.VisualisationType_Grid, StringComparison.OrdinalIgnoreCase))
                    {
                        AState.OutputObj.Information[0] = ds_common.Utilities.VisualisationType_Grid;
                    }
                }
                //********]


                AState.HasInitialisationCompleted = true;
                AState.ofr.ProcessingPoint = "Initialisation completed";

                if (IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.Success;
                    return ret;
                }
            }
            //***************]



            // Determine if this the incremental data processing has completed, by checking if no more data have been provided
            if (AState.HasInitialisationCompleted == true & IDO == null)
            {
                if (AState.DataExtractComplete == false)
                {
                    AState.DataExtractComplete = true;
                    AState.DataExtractCompleteDT = DateTime.Now;
                }
            }





            //[**************** If there are still more data points to accummulate then do the accummulation stuff here
            if (AState.DataExtractComplete == false)
            {
                if (IDO == null || IDO.IDO == null)
                {
                    ret.PrimaryReturnValue = RefReturnValues.ErrorInCodeOutsideOfThisFunction;
                    return ret;
                }



                string ct;
                int FieldID = -1;




                //[************************ Check if the mandatory parameters have been provided

                ct = ds_common.Utilities.GetConfigParameter("field_id", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No field_id parameter specified";
                    return ret;
                }
                else
                {
                    if (int.TryParse(ct, out FieldID))
                    {
                        if (FieldID <= 0)
                        {
                            ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                            ret.AdditionalInfo = "field_id parameter has a zero or negative value";
                            return ret;
                        }
                    }
                    else
                    {
                        ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                        ret.AdditionalInfo = "Couldn't parse the field_id parameter value into an integer";
                        return ret;
                    }
                }

                ct = ds_common.Utilities.GetConfigParameter("interpolation_method", ref AState);
                if (string.IsNullOrEmpty(ct))
                {
                    ret.PrimaryReturnValue = RefReturnValues.InvalidInputParameters;
                    ret.AdditionalInfo = "No interpolation_method parameter specified";
                    return ret;
                }
                //************************]


                //[***************************** Do the relevant processing
                if (IDO.SourceIsText)
                {
                    // Process text-based incremental data objects
                    double v;
                    List<double> ld;

                    List<object> LFields = (List<object>)IDO.IDO;



                    if (FieldID > LFields.Count)
                    {
                        AState.ofr.PrimaryReturnValue = RefReturnValues.InvalidConfigurationSpecification;
                        ret.AdditionalInfo = "Field ID used to calculate the arithmetic mean is outside of the range of available fields";
                    }
                    else
                    {
                        if (double.TryParse((string)LFields[FieldID - 1], out v))
                        {
                            ld = (List<double>)AState.IntermediateObj[0];
                            ld.Add(v);
                            AState.IntermediateObj[0] = (object)ld;
                        }
                    }

                }
                else
                {
                    // Process binary-based incremental data objects
                }


                //*****************************]



                ret.PrimaryReturnValue = RefReturnValues.Success;
                return ret;
            }
            //****************]






            //[********************** Do any final (end of incremental data provision) processing here
            if (AState.DataExtractComplete == true)
            {
                bool Cont = true;


                List<double> ld = (List<double>)AState.IntermediateObj[0];

                // Catch the scenario where data couldn't be correctly processed due to a configuration issue
                if (AState.ofr.PrimaryReturnValue == RefReturnValues.InvalidConfigurationSpecification)
                {
                    const string e = "Field ID used to calculate the median is outside of the range of available fields";
                    AState.ofr.ErrorMessage = e;
                    AState.OutputMessagesToUser.Add(e);
                    Cont = false;
                }

                // Catch the scenario where no data were accummulated to analyse
                if (ld.Count == 0)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    AState.ofr.ErrorMessage = "No values were obtained to calculate a median over";
                    Cont = false;
                }

                if (ld.Count == 1)
                {
                    AState.ofr.PrimaryReturnValue = RefReturnValues.ConditionalSuccess;
                    if (AState.ofr.AdditionalReturnValues == null) AState.ofr.AdditionalReturnValues = new List<RefReturnValues>();
                    AState.ofr.AdditionalReturnValues.Add(RefReturnValues.ErrorInExternalFile);
                    AState.ofr.ErrorMessage = "No values were obtained to calculate a median over";
                    Cont = false;
                }

                if (Cont)
                {
                    double Median = 0.0d;
                    long ldcount = ld.Count;
                    string ct;
                    bool Foundmv = false;
                    ct = ds_common.Utilities.GetConfigParameter("interpolation_method", ref AState);
                    if (ct == null) ct = string.Empty;


                    // Sort the list
                    ld.Sort();


                    if (ldcount % 2 == 0)
                    {
                        switch (ct.ToLowerInvariant())
                        {
                            case "none":
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                AState.OutputMessagesToUser.Add("No median was calculated because the command configuration was set to not interpolate if there is an even number of elements, which there are.");
                                break;

                            case "choose lower":
                                Median = ld[((int)(ldcount / 2.0d) - 1)];
                                Foundmv = true;
                                break;

                            case "choose higher":
                                Median = ld[(((int)(ldcount / 2.0d) - 1) + 1)];
                                Foundmv = true;
                                break;

                            case "nearest neighbour average":
                                Median = ((ld[(((int)(ldcount / 2.0d) - 1) + 1)]) + (ld[((int)(ldcount / 2.0d) - 1)])) / 2.0d;
                                Foundmv = true;
                                break;

                            case "local neighbourhood quadratic":
                                AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                AState.OutputMessagesToUser.Add("The \"LocalNeighbourhoodQuadratic\" configuration for median calculation is not currently supported.");
                                break;

                            default:
                                //AState.ofr.PrimaryReturnValue = RefReturnValues.OtherError;
                                //AState.OutputMessagesToUser.Add("No valid interpolation method was specified and there was an even number of elements.");
                                Median = ((ld[(((int)(ldcount / 2.0d) - 1) + 1)]) + (ld[((int)(ldcount / 2.0d) - 1)])) / 2.0d;
                                Foundmv = true;
                                break;
                        }
                    }
                    else
                    {
                        Median = ld[((int)(ldcount / 2.0d) - 1) + 1];
                        Foundmv = true;
                    }


                    if (Foundmv)
                    {
                        if (AState.OutputObj.Information[0] == ds_common.Utilities.VisualisationType_Grid)
                        {
                            // Set the grid headers appropriately
                            AState.SourceOriginalFieldHeaders = new List<string>();

                            // Set the output value
                            AState.OutputObj.PrimaryOutput.Add(new List<object>() { (object)Median.ToString() });
                        }
                        else
                        {
                            AState.OutputObj.PrimaryOutput.Add((object)Median.ToString());
                        }
                    }




                }
            }
            //**********************]




            // Do the necessary things here to display the outputs

            //if (AState.ofr.PrimaryReturnValue == RefReturnValues.Success)
            //{
            //    // Return the output as the answer

            //}
            //else
            //{
            //    // Return the output as the error message, etc
            //}




            AState.Completed = true;
            AState.CompletedDT = DateTime.Now;


            // If no other return value has been set, then set one of success
            if (AState.ofr.PrimaryReturnValue == RefReturnValues.Indeterminate) AState.ofr.PrimaryReturnValue = RefReturnValues.Success;



            ret.PrimaryReturnValue = RefReturnValues.Success;
            return ret;
        }











        // Populate an OFR object with specific values
        static void CreateOFRErrorResponse(ref ofr OFRToFill, RefReturnValues PrimaryReturnValue, string ProcessingPoint, string AdditionalInfo, List<RefReturnValues> AdditionalReturnValues = null, Exception e = null)
        {
            OFRToFill.DT = DateTime.Now;
            OFRToFill.PrimaryReturnValue = PrimaryReturnValue;
            OFRToFill.AdditionalReturnValues = AdditionalReturnValues;
            OFRToFill.ProcessingPoint = ProcessingPoint;
            OFRToFill.AdditionalInfo = AdditionalInfo;
            if (e != null)
            {
                OFRToFill.ErrorMessage = e.Message;
                if (e.InnerException != null)
                {
                    OFRToFill.ErrorDetails = e.InnerException.ToString();
                }
                else
                {
                    OFRToFill.ErrorDetails = string.Empty;
                }
            }
            else
            {
                OFRToFill.ErrorMessage = string.Empty;
                OFRToFill.ErrorDetails = string.Empty;
            }
        }












    }










    public class OCommandConfig_ANALYSE_Arithmetic_ArithmeticMean
    {
        public uint FieldID;
    }

    public class OCommandOutput_ANALYSE_Arithmetic_ArithmeticMean
    {
        public bool CalculationSuccessful;
        public double ArithmeticMeanValue;
        public double RunningSum;
        public int Count;
    }









    public class OCommandConfig_ANALYSE_Arithmetic_Median
    {
        public uint FieldID;
        public MedianInterpolationMethod InterpolationMethod;
    }

    public class OCommandOutput_ANALYSE_Arithmetic_Median
    {
        public bool CalculationSuccessful;
        public double MedianValue;
        public List<double> LValues;
    }


    public enum MedianInterpolationMethod
    {
        NoInterpolation_Fail = 0,
        NoInterpolation_ChooseLower = 1,
        NoInterpolation_ChooseHigher = 2,
        NearestNeighbourAverage = 3,
        LocalNeighbourhoodQuadratic = 4
    }



}
