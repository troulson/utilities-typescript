import { DynamoDB } from "aws-sdk";

/**
 * A class that represents a write buffer for DynamoDB.
 */
export class WriteBuffer {

    // The name of the DynamoDB table to write to
    private table: string;

    // The instance of the DynamoDB client to use
    private dynamoDbInstance: DynamoDB;

    // The maximum number of items to write in a single batch
    private maxWriteBatchSize: number;

    // The buffer that holds the items to be written
    private buffer: Array<any> = new Array<any>();

    /**
     * Creates a new WriteBuffer instance.
     *
     * @param table The name of the DynamoDB table to write to.
     * @param dynamoDbInstance The instance of the DynamoDB client to use.
     * @param maxWriteBatchSize The maximum number of items to write in a single batch (default: 25).
     */
    constructor(table: string, dynamoDbInstance: DynamoDB, maxWriteBatchSize: number = 25) {
        this.table = table;
        this.dynamoDbInstance = dynamoDbInstance;
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    /**
     * Adds an item to the buffer and writes it to DynamoDB if the buffer is full.
     *
     * @param requestItem The item to add to the buffer.
     * @param onFlush An optional callback function to execute after flushing the buffer.
     */
    public async put(requestItem: any, onFlush: () => any = () => {}) {

        // Add the item to the buffer
        this.buffer.push(requestItem);

        // If the buffer is full, flush it and call the onFlush callback function
        if (this.buffer.length === this.maxWriteBatchSize) {
            await this.flush();
            await onFlush();
        }
    }

    /**
     * Returns the current length of the buffer.
     *
     * @returns {number} - The buffer length.
     */
    public length(): number {
        return this.buffer.length;
    }

    /**
     * Writes all items in the buffer to DynamoDB and clears the buffer.
     */
    public async flush() {

        // Only flush if the buffer has items in it
        if (this.buffer.length > 0) {
            await new Promise<void>((resolve, reject) => {

                // Call batchWriteItem on the DynamoDB instance with the generated parameters
                this.dynamoDbInstance.batchWriteItem(this.params(), err => {
                    if (err) {
                        reject(err);

                    } else {
                        // Clear the buffer
                        this.buffer = new Array<any>();

                        resolve()
                    }
                });

            }).catch(err => {
                console.error("Error: " + err.message);

                throw err;
            });
        }

    }

    /**
     * Generates the parameters for the batchWriteItem call to DynamoDB.
     *
     * @return {RequestItems: {[p: string]: any[]}} the parameters for the write request.
     */
    private params() {

        // Map each item in the buffer to a PutRequest object
        const requestItems: Array<any> = this.buffer.map((requestItem) => {
            return {
                PutRequest: {
                    Item: requestItem
                }
            };
        });

        // Return an object with a RequestItems property containing an
        // array of PutRequest objects for each item in the buffer
        return {
            RequestItems: {
                [this.table]: requestItems
            }
        }
    }

}