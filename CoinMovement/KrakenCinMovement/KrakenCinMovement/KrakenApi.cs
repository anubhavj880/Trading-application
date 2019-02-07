using Newtonsoft.Json;
using System.Collections.Generic;

namespace KrakenCoinMovement
{
    public class ResponseBase
    {
        public List<string> Error;
    }

    public class GetBalanceResponse : ResponseBase
    {
        public Dictionary<string, decimal> Result;
    }

    public class TradeBalanceInfo
    {
        /// <summary>
        /// Equivalent balance(combined balance of all currencies).
        /// </summary>
        [JsonProperty(PropertyName = "eb")]
        public decimal EquivalentBalance;

        /// <summary>
        /// Trade balance(combined balance of all equity currencies).
        /// </summary>
        [JsonProperty(PropertyName = "tb")]
        public decimal TradeBalance;

        /// <summary>
        /// Margin amount of open positions.
        /// </summary>
        [JsonProperty(PropertyName = "m")]
        public decimal MarginAmount;

        /// <summary>
        /// Unrealized net profit/loss of open positions.
        /// </summary>
        [JsonProperty(PropertyName = "n")]
        public decimal UnrealizedProfitAndLoss;

        /// <summary>
        /// Cost basis of open positions.
        /// </summary>
        [JsonProperty(PropertyName = "c")]
        public decimal CostBasis;

        /// <summary>
        /// Current floating valuation of open positions.
        /// </summary>
        [JsonProperty(PropertyName = "v")]
        public decimal FloatingValutation;

        /// <summary>
        /// Equity = trade balance + unrealized net profit/loss.
        /// </summary>
        [JsonProperty(PropertyName = "e")]
        public decimal Equity;

        /// <summary>
        /// Free margin = equity - initial margin(maximum margin available to open new positions).
        /// </summary>
        [JsonProperty(PropertyName = "mf")]
        public decimal FreeMargin;

        /// <summary>
        /// Margin level = (equity / initial margin) * 100
        /// </summary>
        [JsonProperty(PropertyName = "ml")]
        public decimal MarginLevel;
    }

    public class GetTradeBalanceResponse : ResponseBase
    {
        public TradeBalanceInfo Result;
    }

    public class GetDepositMethodsResult
    {
        /// <summary>
        /// Name of deposit method.
        /// </summary>
        public string Method;

        /// <summary>
        /// Maximum net amount that can be deposited right now, or false if no limit
        /// </summary>
        public string Limit;

        /// <summary>
        /// Amount of fees that will be paid.
        /// </summary>
        public string Fee;

        /// <summary>
        /// Whether or not method has an address setup fee (optional).
        /// </summary>
        [JsonProperty(PropertyName = "address-setup-fee")]
        public bool? AddressSetupFee;
    }

    public class GetDepositMethodsResponse : ResponseBase
    {
        public GetDepositMethodsResult[] Result;
    }

    public class GetDepositAddressesResult
    {
    }

    public class GetDepositAddressesResponse : ResponseBase
    {
        public GetDepositAddressesResult Result;
    }

    public class GetDepositStatusResult
    {
        /// <summary>
        /// Name of the deposit method used.
        /// </summary>
        public string Method;

        /// <summary>
        /// Asset class.
        /// </summary>
        public string Aclass;

        /// <summary>
        /// Asset X-ISO4217-A3 code.
        /// </summary>
        public string Asset;

        /// <summary>
        /// Reference id.
        /// </summary>
        public string RefId;

        /// <summary>
        /// Method transaction id.
        /// </summary>
        public string Txid;

        /// <summary>
        /// Method transaction information.
        /// </summary>
        public string Info;

        /// <summary>
        /// Amount deposited.
        /// </summary>
        public decimal Amount;

        /// <summary>
        /// Fees paid.
        /// </summary>
        public decimal Fee;

        /// <summary>
        /// Unix timestamp when request was made.
        /// </summary>
        public int Time;

        /// <summary>
        /// status of deposit
        /// </summary>
        public string Status;

        // status-prop = additional status properties(if available)
        //    return = a return transaction initiated by Kraken
        //    onhold = deposit is on hold pending review
    }

    public class GetDepositStatusResponse : ResponseBase
    {
        public GetDepositStatusResult[] Result;
    }

    public class GetWithdrawInfoResult
    {
        /// <summary>
        /// Name of the withdrawal method that will be used
        /// </summary>
        public string Method;

        /// <summary>
        /// Maximum net amount that can be withdrawn right now.
        /// </summary>
        public decimal Limit;

        /// <summary>
        /// Amount of fees that will be paid.
        /// </summary>
        public decimal Fee;
    }

    public class GetWithdrawInfoResponse : ResponseBase
    {
        public GetWithdrawInfoResult Result;
    }

    public class WithdrawResult
    {
        public string RefId;
    }

    public class WithdrawResponse : ResponseBase
    {
        public WithdrawResult Result;
    }

    public class GetWithdrawStatusResult
    {
        /// <summary>
        /// Name of the withdrawal method used.
        /// </summary>
        public string Method;

        /// <summary>
        /// Asset class.
        /// </summary>
        public string Aclass;

        /// <summary>
        /// Asset X-ISO4217-A3 code.
        /// </summary>
        public string Asset;

        /// <summary>
        /// Reference id.
        /// </summary>
        public string RefId;

        /// <summary>
        /// Method transaction id.
        /// </summary>
        public string Txid;

        /// <summary>
        /// Method transaction information.
        /// </summary>
        public string Info;

        /// <summary>
        /// Amount withdrawn.
        /// </summary>
        public decimal Amount;

        /// <summary>
        /// Fees paid.
        /// </summary>
        public decimal Fee;

        /// <summary>
        /// Unix timestamp when request was made.
        /// </summary>
        public int Time;

        /// <summary>
        /// Status of withdrawal.
        /// </summary>
        public string Status;

        //status-prop = additional status properties(if available).
        //cancel-pending = cancelation requested.
        //canceled = canceled.
        //cancel-denied = cancelation requested but was denied.
        //return = a return transaction initiated by Kraken; it cannot be canceled.
        //onhold = withdrawal is on hold pending review.
    }

    public class GetWithdrawStatusResponse : ResponseBase
    {
        public GetWithdrawStatusResult Result;
    }

    public class WithdrawCancelResponse : ResponseBase
    {
        public bool Result;
    }


}