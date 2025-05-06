import pandas as pd
import os
import json
from datetime import datetime


class SimpleDataValidator:
    def __init__(self, data_path):
        self.data_path = data_path
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'checks': [],
            'overall_success': True
        }

    def load_data(self):
        """파케이 파일 또는 CSV 파일 로드"""
        if os.path.isfile(self.data_path):
            if self.data_path.endswith('.parquet'):
                return pd.read_parquet(self.data_path)
            elif self.data_path.endswith('.csv'):
                return pd.read_csv(self.data_path)
        else:
            # 디렉토리에서 파일 검색
            if not os.path.exists(self.data_path):
                raise ValueError(f"경로가 존재하지 않습니다: {self.data_path}")

            files = os.listdir(self.data_path)
            if not files:
                raise ValueError(f"디렉토리가 비어 있습니다: {self.data_path}")

            # 샘플 파일 선택
            sample_files = [f for f in files if f.endswith('.parquet') or f.endswith('.csv')]
            if not sample_files:
                raise ValueError(f"지원되는 파일 형식이 없습니다: {self.data_path}")

            sample_file = os.path.join(self.data_path, sample_files[0])
            print(f"샘플 파일 로드 중: {sample_file}")

            if sample_file.endswith('.parquet'):
                return pd.read_parquet(sample_file)
            elif sample_file.endswith('.csv'):
                return pd.read_csv(sample_file)

    def check_not_null(self, df, column_name, description=""):
        """컬럼의 null 값 검사"""
        null_count = df[column_name].isnull().sum()
        success = null_count == 0

        result = {
            'check_name': f'not_null_{column_name}',
            'description': description or f"'{column_name}' 컬럼에 null 값이 없어야 합니다",
            'success': success,
            'details': {
                'null_count': int(null_count),
                'total_count': len(df)
            }
        }

        self.validation_results['checks'].append(result)
        if not success:
            self.validation_results['overall_success'] = False

        return success

    def check_value_range(self, df, column_name, min_value, max_value, description=""):
        """컬럼 값의 범위 검사"""
        # null 값 제외하고 검사
        valid_df = df[df[column_name].notnull()]

        if len(valid_df) == 0:
            # 유효한 데이터가 없는 경우
            result = {
                'check_name': f'range_{column_name}_{min_value}_{max_value}',
                'description': description or f"'{column_name}' 컬럼의 값이 {min_value}~{max_value} 범위 내에 있어야 합니다",
                'success': False,
                'details': {
                    'error': '유효한 데이터가 없습니다',
                    'total_count': len(df)
                }
            }
            self.validation_results['overall_success'] = False
        else:
            out_of_range = ((valid_df[column_name] < min_value) | (valid_df[column_name] > max_value)).sum()
            success = out_of_range == 0

            result = {
                'check_name': f'range_{column_name}_{min_value}_{max_value}',
                'description': description or f"'{column_name}' 컬럼의 값이 {min_value}~{max_value} 범위 내에 있어야 합니다",
                'success': success,
                'details': {
                    'out_of_range_count': int(out_of_range),
                    'total_count': len(valid_df)
                }
            }

            if not success:
                self.validation_results['overall_success'] = False

        self.validation_results['checks'].append(result)
        return result['success']

    def check_unique_values(self, df, column_name, description=""):
        """컬럼의 고유 값 개수 확인"""
        unique_count = df[column_name].nunique()
        total_count = len(df)

        # 정보 제공 목적의 체크 (성공/실패 판단 없음)
        result = {
            'check_name': f'unique_{column_name}',
            'description': description or f"'{column_name}' 컬럼의 고유 값 개수",
            'success': True,  # 항상 성공
            'details': {
                'unique_count': int(unique_count),
                'total_count': total_count,
                'unique_ratio': float(unique_count / total_count) if total_count > 0 else 0
            }
        }

        self.validation_results['checks'].append(result)
        return True

    def save_results(self, output_path="validation_results.json"):
        """검증 결과 저장"""
        with open(output_path, 'w') as f:
            json.dump(self.validation_results, f, indent=2)

        print(f"검증 결과가 {output_path}에 저장되었습니다.")
        print(f"전체 검증 결과: {'성공' if self.validation_results['overall_success'] else '실패'}")

        return self.validation_results


def run_weather_data_validation(data_path='../output/weather_data'):
    """날씨 데이터 검증 실행"""
    validator = SimpleDataValidator(data_path)

    try:
        # 데이터 로드
        print(f"{data_path}에서 데이터 로드 중...")
        df = validator.load_data()
        print(f"데이터 로드 완료: {len(df)}개 행")

        # 필수 필드 null 검사
        validator.check_not_null(df, 'city', "도시명은 필수 항목입니다")
        validator.check_not_null(df, 'temperature', "온도 값은 필수 항목입니다")
        validator.check_not_null(df, 'humidity', "습도 값은 필수 항목입니다")

        # 값 범위 검사
        validator.check_value_range(df, 'temperature', -50, 60, "온도는 -50°C에서 60°C 사이여야 합니다")
        validator.check_value_range(df, 'humidity', 0, 100, "습도는 0%에서 100% 사이여야 합니다")

        # 통계 정보
        validator.check_unique_values(df, 'city')

        # 결과 저장
        results = validator.save_results()

        return results

    except Exception as e:
        print(f"데이터 검증 중 오류 발생: {str(e)}")
        validator.validation_results['overall_success'] = False
        validator.validation_results['error'] = str(e)
        validator.save_results('validation_error.json')
        return validator.validation_results


if __name__ == "__main__":
    run_weather_data_validation()