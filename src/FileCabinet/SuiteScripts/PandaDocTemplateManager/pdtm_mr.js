/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(
    [
        'N/search',
        'N/record',
        'N/https'
    ],
    (
        search,
        record,
        https
    ) => {
        const RATE_THROTTLE_BUFFER = 1000; //milliseconds
        const PANDA_URL = 'https://api.pandadoc.com/public/v1/templates'
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            let templates = [];
            let page = 1;
            let response = templateRequest(page);

            while(response.results.length > 0){
                templates.push(...response['results'])
                page++;
                response = templateRequest(page)
            }

            return templates.map(template => template['id']);
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            const url = `${PANDA_URL}/${mapContext.value}/details`

            let headers = {
                'Authorization': 'API-Key ad43a32afdcab146d612813cbc2464253d64f4a9',
                'Accept': '*/*'
            };
            //wait(RATE_THROTTLE_BUFFER);
            const response = https.get({ url: url, headers: headers });
            const jsonResponse = JSON.parse(response.body);
            let templateId;
            search.create({
                type: 'customrecord_pdtm_template',
                filters: [
                    ['custrecord_pdtm_template_id', search.Operator.IS, jsonResponse['id']]
                ],
                columns: []
            }).run().each(result => {
                templateId = result.id;
                jsonResponse['netsuite_id'] = result.id;
                return true;
            });
            if (templateId === undefined) {
                const templateRecord = record.create({
                    type: 'customrecord_pdtm_template'
                })
                templateRecord.setValue({
                    fieldId: 'name',
                    value: jsonResponse.name
                });
                templateRecord.setValue({
                    fieldId: 'custrecord_pdtm_template_id',
                    value: jsonResponse.id
                });
                templateRecord.setValue({
                    fieldId: 'custrecord_pdtm_template_modify_date',
                    value: new Date(jsonResponse.date_modified)
                });
                jsonResponse['netsuite_id'] = templateRecord.save()
            }

            mapContext.write({
                key: templateId,
                value: jsonResponse,
            })
        }

        const wait = (timeToWait) => {
            const start = Date.now();
            let end = Date.now();
            while(end < start + timeToWait){
                end = Date.now();
            }
            end = Date.now()
        }

        const templateRequest = (page) => {
            let headers = {
                'Authorization': 'API-Key ad43a32afdcab146d612813cbc2464253d64f4a9',
                'Accept': '*/*'
            };
            return JSON.parse(https.get({
                url:     `${PANDA_URL}?count=100&page=${page}`,
                headers: headers
            }).body);
        }
        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {

            const templateData = JSON.parse(reduceContext.values);
            const netsuiteTemplateId = reduceContext.key;
            const tokens = templateData['tokens'].map(template => template['name']);
            let nsTemplateTokens = []
            search.create({
                type: 'customrecord_pdtm_template_detail',
                filters: [
                    ['isinactive', search.Operator.IS, false],
                    'AND',
                    ['custrecord_pdtm_template_detail_parent', search.Operator.IS, netsuiteTemplateId]
                ],
                columns: ['custrecord_pdtm_template_detail_pd_field', 'custrecord_pdtm_template_detail_ns_field']
            }).run().each(result => {
                nsTemplateTokens.push(
                    result.getValue('custrecord_pdtm_template_detail_pd_field')
                )
                return true;
            })
            const unmappedFields = tokens.filter( token => {
                return !nsTemplateTokens.includes(token)
            });
            unmappedFields.forEach( field => {
                let templateDetailRecord = record.create({
                    type: 'customrecord_pdtm_template_detail'
                });
                templateDetailRecord.setValue({
                    fieldId: 'custrecord_pdtm_template_detail_parent',
                    value: netsuiteTemplateId
                });
                templateDetailRecord.setValue({
                    fieldId: 'custrecord_pdtm_template_detail_pd_field',
                    value: field
                })
                templateDetailRecord.save();
            })

            return reduceContext;
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {

        }

        return {getInputData, map, reduce, summarize}

    });
