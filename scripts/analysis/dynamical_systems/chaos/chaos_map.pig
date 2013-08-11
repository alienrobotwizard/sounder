register '../../../../udf/target/sounder-1.0-SNAPSHOT.jar';
register '$COMMONS_MATH_JAR';

define LyapunovForHenon sounder.pig.chaos.LyapunovForHenon();

points = load 'data/space_spec' using sounder.pig.points.RectangularSpaceLoader('20');

exponents = foreach points generate $0 as a, $1 as b, LyapunovForHenon($0, $1);         
 
store exponents into 'data/result';
