/**
 * Enumeration of possible circuit breaker states.
 */
enum State {
    Open = 0,
    HalfOpen = 1,
    Closed = 2
}

/**
 * A class that implements a circuit breaker design pattern to prevent cascading failures.
 */
export class CircuitBreaker {

    // The timeout duration for the circuit breaker in milliseconds.
    private timeout: number;

    // The number of recent invocations to retain for determining the failure rate.
    private retentionSize: number;

    /*
       An array of recent invocations, where true represents a failed invocation, false represents
       a successful invocation, and null represents an invocation that has not yet been completed.
     */
    private failureRetention: Array<boolean | null>;

    // The threshold failure rate at which the circuit breaker should open.
    private threshold: number;

    // A boolean indicating whether the circuit breaker should hold invocations while in the open state.
    private holdInvocations: boolean;

    // The current state of the circuit breaker.
    private state: State = State.Closed;

    // The timestamp in milliseconds for when the circuit breaker can enter a half-open state.
    private timeoutEnd: number = 0;

    // The current failure rate of recent invocations.
    private retentionFailureRate: number = 0;

    /**
     * Creates a new CircuitBreaker instance.
     *
     * @param timeout The timeout duration for the circuit breaker in milliseconds (default: 1000).
     * @param retentionSize The number of recent invocations to retain for determining the failure rate (default: 10).
     * @param threshold The threshold failure rate at which the circuit breaker should open (default: 0.5).
     * @param holdInvocations A boolean indicating whether the circuit breaker should hold invocations while
     * in the open state (default: true).
     */
    constructor(
        timeout: number = 1000,
        retentionSize: number = 10,
        threshold: number = 0.5,
        holdInvocations: boolean = true
    ) {
        this.timeout = timeout;
        this.retentionSize = retentionSize;
        this.threshold = threshold;
        this.holdInvocations = holdInvocations;

        // Initialise the failure retentions to null
        this.failureRetention = new Array(retentionSize).fill(null);
    }

    /**
     * Invokes a function protected by the circuit breaker.
     *
     * @param {Function} callback - The function to be invoked.
     *
     * @returns {Promise<any>} - A Promise that resolves with the result of the function.
     *
     * @throws {Error} - Throws an error if the circuit breaker is open and holdInvocations
     * is false or the callback fails.
     *
     * @memberof CircuitBreaker
     */
    public async invoke(callback: () => Promise<any>): Promise<any> {

        // If the circuit breaker is open, check if it's time to transition to half-open state
        if (this.state === State.Open) {
            if (this.remainingTimeoutDuration() <= 0) {
                this.halfOpen();
            }

            // If the circuit breaker is still open and holdInvocations is false, throw an error
            if (this.state === State.Open && !this.holdInvocations) {
                throw Error('Circuit breaker is open');
            }

            // Wait for the remaining timeout duration
            await new Promise(resolve => setTimeout(resolve,
                this.remainingTimeoutDuration()));

            // Transition to half-open state
            this.halfOpen();
        }

        // Invoke the callback function and return
        return await callback().then(data => {

            // If the function call succeeded, update the circuit breaker and return the data
            this.update(false);

            return data;

        }).catch(err => {
            // If the function call failed, update the circuit breaker and throw the error
            this.update(true);

            throw err;
        });

    }

    /**
     * Invokes a given URL and handles the response.
     *
     * @param {string} url - The URL to invoke.
     * @param {number} [timeout=1000] - The timeout duration for the request.
     * @param {Set<number>} [excludeStatusErrors=new Set<number>()] - Set of HTTP status
     * codes to exclude from triggering a circuit breaker failure.
     * @param {boolean} [requireJson=true] - If true, expects the response to be valid JSON
     * syntax and returns the parsed JSON data. Otherwise, returns the raw response object.
     * @param responseValidator callback function that takes the response as a parameter and returns a boolean
     * value indicating whether it is a valid response or not.
     *
     * @returns {Promise<any>} - A promise that resolves with the response data or rejects with an error message.
     */
    public async invokeUrl(
        url: string,
        timeout: number = 1000,
        excludeStatusErrors: Set<number> = new Set<number>(),
        requireJson: boolean = true,
        responseValidator: (resp: any) => Promise<void> = () => Promise.resolve()
    ): Promise<any> {

        // Invokes the given callback function within the circuit breaker.
        return await this.invoke(async () => {
            let response;

            // Invokes a fetch request with the given URL and timeout duration, and aborts
            // the request if the timeout duration is exceeded.
            try {
                response = await fetch(url, { signal: AbortSignal.timeout(timeout)});

            } catch (err) {
                // If there is an error, emit it
                throw (err);
            }

            // If the response is valid and has a permitted status code
            if (response instanceof Response && (
                response.ok || excludeStatusErrors.has(response.status))) {

                // If requireJson is true, attempts to parse the response body as JSON data.
                if (requireJson) {
                    try {
                        response = await response.json();

                    } catch(err) {
                        throw new Error('Response is invalid JSON syntax');
                    }
                }

                // Check if response is a valid response for the requester
                await responseValidator(response);

                return response;
            }

            // If the response is invalid or has forbidden status code, rejects with the status text
            throw new Error(response.statusText);

        });

    }

    /**
     * Calculates the remaining time until the current timeout ends.
     *
     * @returns {number} The remaining time in milliseconds.
     *
     * @private
     */
    private remainingTimeoutDuration(): number {
        return this.timeoutEnd - (new Date()).getTime();
    }

    /**
     * Sets the circuit breaker state to Half Open.
     *
     * @private
     */
    private halfOpen() {
        this.state = State.HalfOpen;

        console.info('Circuit breaker state change: HALF OPEN');
    }

    /**
     * Sets the circuit breaker state to Closed.
     *
     * @private
     */
    private close() {
        this.state = State.Closed;

        console.info('Circuit breaker state change: CLOSED');
    }

    /**
     * Sets the circuit breaker state to Open, resets the failure retention array and failure rate,
     * and sets the timeout end time based on the current time and the specified timeout duration.
     *
     * @private
     */
    private open() {
        this.state = State.Open;

        console.warn('Circuit breaker state change: OPEN');

        // Reset the failure retention array and the failure rate
        this.failureRetention = new Array(this.retentionSize).fill(null);
        this.retentionFailureRate = 0;

        // Set the timeout end time
        this.timeoutEnd = (new Date()).getTime() + this.timeout;
    }

    /**
     * Updates the circuit breaker state based on the success or failure of the latest invocation.
     *
     * @param {boolean} failed - Whether the latest invocation failed.
     *
     * @private
     */
    private update(failed: boolean) {

        // Remove the oldest failure status from the retention array and add the latest one
        const removed = this.failureRetention.shift();

        this.failureRetention.push(failed);

        // Adjust the failure rate based on the removed and added statuses
        if (removed) {
            this.retentionFailureRate -= (1 / this.retentionSize);
        }

        if (failed) {
            this.retentionFailureRate += (1 / this.retentionSize);
        }

        // If the failure rate has reached or exceeded the threshold, open the circuit breaker
        if (this.retentionFailureRate >= this.threshold) {
            this.open();

        // Otherwise if the circuit breaker is in Half Open state and minimum number of attempts has been met, close it
        } else if (removed !== null && this.state === State.HalfOpen) {
            this.close();
        }
    }

}