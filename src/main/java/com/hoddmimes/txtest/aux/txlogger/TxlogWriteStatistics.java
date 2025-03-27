package com.hoddmimes.txtest.aux.txlogger;

import org.HdrHistogram.Histogram;


public class TxlogWriteStatistics
{
	Histogram mStat;
	long mTotalOpenTime;
	long mTotalOpens;
	long mMin, mMax;



	public TxlogWriteStatistics() {
		mStat = new Histogram(0);
		mTotalOpens = mTotalOpenTime = 0;
	}

	public void addOpen( long pTxOpenTimeUsec ) {
		mTotalOpens++;
		mTotalOpenTime += pTxOpenTimeUsec;
	}

	public void add( long pTxWriteTimeUsec ) {
		mStat.recordValue( pTxWriteTimeUsec );
	}

	public String toString() {
		long tMin = mStat.getMinValue();
		long tMax = mStat.getMaxValue();
		long tMean = (long) mStat.getMean();
		long tStdDev95 = mStat.getValueAtPercentile( 95.00d );


		if (mTotalOpens > 0) {
			return "count: " + mStat.getTotalCount() + " mean: " + tMean + " usec 95% " + tStdDev95 + " min: " + tMin + " max: " + tMax +
					"  file open: " + mTotalOpens + " mean open: " + (mTotalOpenTime / mTotalOpens) + " usec";
		}
		return "count: " + mStat.getTotalCount() + " mean: " + tMean + " usec 95% " + tStdDev95 + " min: " + tMin + " max: " + tMax;
	}

}
